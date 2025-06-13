from collections.abc import Callable
from typing import Any
import pyarrow as pa
from orcabridge.types import Packet
from .core import TypeHandler, TypeInfo


class TypeRegistry:
    """Registry that manages type handlers with semantic type names."""

    def __init__(self):
        self._handlers: dict[
            type, tuple[TypeHandler, str]
        ] = {}  # Type -> (Handler, semantic_name)
        self._semantic_handlers: dict[str, TypeHandler] = {}  # semantic_name -> Handler

    def register(
        self,
        semantic_name: str,
        handler: TypeHandler,
        explicit_types: type | tuple[type, ...] | None = None,
        override: bool = False,
    ):
        """Register a handler with a semantic type name.

        Args:
            semantic_name: Identifier for this semantic type (e.g., 'path', 'uuid')
            handler: The type handler instance
            explicit_types: Optional override of types to register for (if different from handler's supported_types)
            override: If True, allow overriding existing registration for the same semantic name and Python type(s)
        """
        # Determine which types to register for
        if explicit_types is not None:
            types_to_register = (
                explicit_types
                if isinstance(explicit_types, tuple)
                else (explicit_types,)
            )
        else:
            supported = handler.supported_types()
            types_to_register = (
                supported if isinstance(supported, tuple) else (supported,)
            )

        # Register handler for each type
        for python_type in types_to_register:
            if python_type in self._handlers and not override:
                existing_semantic = self._handlers[python_type][1]
                # TODO: handle overlapping registration more gracefully
                raise ValueError(
                    f"Type {python_type} already registered with semantic type '{existing_semantic}'"
                )

            self._handlers[python_type] = (handler, semantic_name)

        # Register by semantic name
        if semantic_name in self._semantic_handlers and not override:
            raise ValueError(f"Semantic type '{semantic_name}' already registered")

        self._semantic_handlers[semantic_name] = handler

    def get_handler(self, python_type: type) -> TypeHandler | None:
        """Get handler for a Python type."""
        handler_info = self._handlers.get(python_type)
        return handler_info[0] if handler_info else None

    def get_semantic_name(self, python_type: type) -> str | None:
        """Get semantic name for a Python type."""
        handler_info = self._handlers.get(python_type)
        return handler_info[1] if handler_info else None

    def get_handler_by_semantic_name(self, semantic_name: str) -> TypeHandler | None:
        """Get handler by semantic name."""
        return self._semantic_handlers.get(semantic_name)

    def __contains__(self, python_type: type) -> bool:
        """Check if a Python type is registered."""
        return python_type in self._handlers


def is_packet_supported(
    packet_type_info: dict[str, type], registry: TypeRegistry
) -> bool:
    """Check if all types in the packet are supported by the registry."""
    return all(python_type in registry for python_type in packet_type_info.values())


def create_packet_converters(
    packet_type_info: dict[str, type], registry: TypeRegistry
) -> tuple[
    Callable[[Packet], dict[str, Any]],
    Callable[[dict[str, Any]], Packet],
]:
    """Create optimized conversion functions for a specific packet type.

    Pre-looks up all handlers to avoid repeated registry lookups during conversion.

    Args:
        type_info: Dictionary mapping parameter names to their Python types
        registry: TypeRegistry containing handlers for type conversions

    Returns:
        Tuple of (to_storage_converter, from_storage_converter) functions

    Raises:
        ValueError: If any type in type_info is not supported by the registry

    Example:
        type_info = {
            'file_path': Path,
            'threshold': float,
            'user_id': UUID
        }

        to_storage, from_storage = create_packet_converters(type_info, registry)

        # Fast conversion (no registry lookups)
        storage_packet = to_storage(original_packet)
        restored_packet = from_storage(storage_packet)
    """

    # Pre-lookup all handlers and validate they exist
    handlers: dict[str, TypeHandler] = {}
    expected_types: dict[str, type] = {}

    for key, python_type in packet_type_info.items():
        handler = registry.get_handler(python_type)
        if handler is None:
            raise ValueError(
                f"No handler registered for type {python_type} (key: '{key}')"
            )

        handlers[key] = handler
        expected_types[key] = python_type

    def to_storage_converter(packet: Packet) -> dict[str, Any]:
        """Convert packet to storage representation.

        Args:
            packet: Dictionary mapping parameter names to Python values

        Returns:
            Dictionary with same keys but values converted to storage format

        Raises:
            KeyError: If packet keys don't match the expected type_info keys
            TypeError: If value type doesn't match expected type
            ValueError: If conversion fails
        """
        # Validate packet keys
        packet_keys = set(packet.keys())
        expected_keys = set(expected_types.keys())

        if packet_keys != expected_keys:
            missing_in_packet = expected_keys - packet_keys
            extra_in_packet = packet_keys - expected_keys

            error_parts = []
            if missing_in_packet:
                error_parts.append(f"Missing keys: {missing_in_packet}")
            if extra_in_packet:
                error_parts.append(f"Extra keys: {extra_in_packet}")

            raise KeyError(
                f"Packet keys don't match expected keys. {'; '.join(error_parts)}"
            )

        # Convert each value
        storage_packet = {}

        for key, value in packet.items():
            expected_type = expected_types[key]
            handler = handlers[key]

            # Handle None values
            if value is None:
                storage_packet[key] = None
                continue

            # Validate value type
            if not isinstance(value, expected_type):
                raise TypeError(
                    f"Value for '{key}' is {type(value).__name__}, expected {expected_type.__name__}"
                )

            # Convert to storage representation
            try:
                storage_value = handler.to_storage_value(value)
                storage_packet[key] = storage_value
            except Exception as e:
                raise ValueError(
                    f"Failed to convert '{key}' of type {expected_type}: {e}"
                ) from e

        return storage_packet

    def from_storage_converter(storage_packet: dict[str, Any]) -> Packet:
        """Convert storage packet back to Python values.

        Args:
            storage_packet: Dictionary with values in storage format

        Returns:
            Dictionary with same keys but values converted back to Python types

        Raises:
            KeyError: If storage_packet keys don't match the expected type_info keys
            ValueError: If conversion fails
        """
        # Validate storage packet keys
        packet_keys = set(storage_packet.keys())
        expected_keys = set(expected_types.keys())

        if packet_keys != expected_keys:
            missing_in_packet = expected_keys - packet_keys
            extra_in_packet = packet_keys - expected_keys

            error_parts = []
            if missing_in_packet:
                error_parts.append(f"Missing keys: {missing_in_packet}")
            if extra_in_packet:
                error_parts.append(f"Extra keys: {extra_in_packet}")

            raise KeyError(
                f"Storage packet keys don't match expected keys. {'; '.join(error_parts)}"
            )

        # Convert each value back
        python_packet = {}

        for key, storage_value in storage_packet.items():
            handler = handlers[key]

            # Handle None values
            if storage_value is None:
                python_packet[key] = None
                continue

            # Convert from storage representation
            try:
                python_value = handler.from_storage_value(storage_value)
                python_packet[key] = python_value
            except Exception as e:
                raise ValueError(f"Failed to convert '{key}' from storage: {e}") from e

        return python_packet

    return to_storage_converter, from_storage_converter


def convert_packet_to_storage(
    packet: Packet, type_info: dict[str, type], registry: TypeRegistry
) -> Packet:
    """Convert a packet to its storage representation using the provided type info.

    Args:
        packet: The original packet to convert
        type_info: Dictionary mapping parameter names to their Python types
        registry: TypeRegistry containing handlers for type conversions

    Returns:
        Converted packet in storage format
    """
    to_storage, _ = create_packet_converters(type_info, registry)
    return to_storage(packet)


def convert_storage_to_packet(
    storage_packet: dict[str, Any], type_info: dict[str, type], registry: TypeRegistry
) -> Packet | None:
    pass


class PacketConverter:
    """
    Convenience class for converting packets between storage and Python formats.
    """

    def __init__(self, packet_type_info: dict[str, type], registry: TypeRegistry):
        """Initialize the packet converter with type info and registry."""
        self._to_storage, self._from_storage = create_packet_converters(
            packet_type_info, registry
        )
        self.packet_type_info = packet_type_info

    def to_storage(self, packet: Packet) -> dict[str, Any]:
        """Convert packet to storage representation."""
        return self._to_storage(packet)

    def from_storage(self, storage_packet: dict[str, Any]) -> Packet:
        """Convert storage packet back to Python values."""
        return self._from_storage(storage_packet)


def convert_packet_to_arrow_table(
    packet: dict[str, Any], type_info: dict[str, type], registry: TypeRegistry
) -> pa.Table:
    """Convert a single packet to a PyArrow Table with one row.

    Args:
        packet: Dictionary mapping parameter names to Python values
        type_info: Dictionary mapping parameter names to their Python types
        registry: TypeRegistry containing handlers for type conversions

    Returns:
        PyArrow Table with the packet data as a single row
    """
    # Get the converter functions
    to_storage, _ = create_packet_converters(type_info, registry)

    # Convert packet to storage format
    storage_packet = to_storage(packet)

    # Create schema
    schema_fields = []
    for key, python_type in type_info.items():
        type_info_obj = registry.extract_type_info(python_type)
        schema_fields.append(pa.field(key, type_info_obj.arrow_type))

    schema = pa.schema(schema_fields)

    # Convert storage packet to arrays (single element each)
    arrays = []
    for field in schema:
        field_name = field.name
        value = storage_packet[field_name]

        # Create single-element array
        array = pa.array([value], type=field.type)
        arrays.append(array)

    # Create table
    return pa.Table.from_arrays(arrays, schema=schema)


def convert_packets_to_arrow_table(
    packets: list[dict[str, Any]], type_info: dict[str, type], registry: TypeRegistry
) -> pa.Table:
    """Convert multiple packets to a PyArrow Table.

    Args:
        packets: List of packets (dictionaries)
        type_info: Dictionary mapping parameter names to their Python types
        registry: TypeRegistry containing handlers for type conversions

    Returns:
        PyArrow Table with all packet data as rows
    """
    if not packets:
        # Return empty table with correct schema
        schema_fields = []
        for key, python_type in type_info.items():
            type_info_obj = registry.extract_type_info(python_type)
            schema_fields.append(pa.field(key, type_info_obj.arrow_type))
        schema = pa.schema(schema_fields)
        return pa.Table.from_arrays([], schema=schema)

    # Get the converter functions (reuse for all packets)
    to_storage, _ = create_packet_converters(type_info, registry)

    # Convert all packets to storage format
    storage_packets = [to_storage(packet) for packet in packets]

    # Create schema
    schema_fields = []
    for key, python_type in type_info.items():
        type_info_obj = registry.extract_type_info(python_type)
        schema_fields.append(pa.field(key, type_info_obj.arrow_type))

    schema = pa.schema(schema_fields)

    # Group values by column
    column_data = {}
    for field in schema:
        field_name = field.name
        column_data[field_name] = [packet[field_name] for packet in storage_packets]

    # Create arrays for each column
    arrays = []
    for field in schema:
        field_name = field.name
        values = column_data[field_name]
        array = pa.array(values, type=field.type)
        arrays.append(array)

    # Create table
    return pa.Table.from_arrays(arrays, schema=schema)


def convert_packet_to_arrow_table_with_field_metadata(
    packet: Packet, type_info: dict[str, type], registry: TypeRegistry
) -> pa.Table:
    """Convert packet to Arrow table with semantic type stored as field metadata."""

    # Get converter
    to_storage, _ = create_packet_converters(type_info, registry)
    storage_packet = to_storage(packet)

    # Create schema fields with metadata
    schema_fields = []
    for key, python_type in type_info.items():
        type_info_obj = registry.extract_type_info(python_type)

        # Create field with semantic type metadata
        field_metadata = {}
        if type_info_obj.semantic_type:
            field_metadata["semantic_type"] = type_info_obj.semantic_type

        field = pa.field(key, type_info_obj.arrow_type, metadata=field_metadata)
        schema_fields.append(field)

    schema = pa.schema(schema_fields)

    # Create arrays
    arrays = []
    for field in schema:
        value = storage_packet[field.name]
        array = pa.array([value], type=field.type)
        arrays.append(array)

    return pa.Table.from_arrays(arrays, schema=schema)


def convert_packets_to_arrow_table_with_field_metadata(
    packets: list[Packet], type_info: dict[str, type], registry: TypeRegistry
) -> pa.Table:
    """Convert multiple packets to Arrow table with field metadata."""

    if not packets:
        return _create_empty_table_with_field_metadata(type_info, registry)

    # Get converter
    to_storage, _ = create_packet_converters(type_info, registry)
    storage_packets = [to_storage(packet) for packet in packets]

    # Create schema with field metadata
    schema = _create_schema_with_field_metadata(type_info, registry)

    # Group values by column
    column_data = {}
    for field in schema:
        field_name = field.name
        column_data[field_name] = [packet[field_name] for packet in storage_packets]

    # Create arrays
    arrays = []
    for field in schema:
        values = column_data[field.name]
        array = pa.array(values, type=field.type)
        arrays.append(array)

    return pa.Table.from_arrays(arrays, schema=schema)


def _create_schema_with_field_metadata(
    type_info: dict[str, type], registry: TypeRegistry
) -> pa.Schema:
    """Helper to create schema with field-level semantic type metadata."""
    schema_fields = []

    for key, python_type in type_info.items():
        type_info_obj = registry.extract_type_info(python_type)

        # Create field metadata
        field_metadata = {}
        if type_info_obj.semantic_type:
            field_metadata["semantic_type"] = type_info_obj.semantic_type

        field = pa.field(key, type_info_obj.arrow_type, metadata=field_metadata)
        schema_fields.append(field)

    return pa.schema(schema_fields)


def _create_empty_table_with_field_metadata(
    type_info: dict[str, type], registry: TypeRegistry
) -> pa.Table:
    """Helper to create empty table with correct schema and field metadata."""
    schema = _create_schema_with_field_metadata(type_info, registry)
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def extract_field_semantic_types(table: pa.Table) -> dict[str, str | None]:
    """Extract semantic type from each field's metadata."""
    semantic_types = {}

    for field in table.schema:
        if field.metadata and b"semantic_type" in field.metadata:
            semantic_type = field.metadata[b"semantic_type"].decode("utf-8")
            semantic_types[field.name] = semantic_type
        else:
            semantic_types[field.name] = None

    return semantic_types


def convert_arrow_table_to_packets_with_field_metadata(
    table: pa.Table, registry: TypeRegistry
) -> list[Packet]:
    """Convert Arrow table back to packets using field metadata."""

    # Extract semantic types from field metadata
    field_semantic_types = extract_field_semantic_types(table)

    # Reconstruct type_info from field metadata
    type_info = {}
    for field in table.schema:
        field_name = field.name
        semantic_type = field_semantic_types.get(field_name)

        if semantic_type:
            # Get handler by semantic type
            handler = registry.get_handler_by_semantic_name(semantic_type)
            if handler:
                python_type = handler.supported_types()
                if isinstance(python_type, tuple):
                    python_type = python_type[0]  # Take first if multiple
                type_info[field_name] = python_type
            else:
                # Fallback to basic type inference
                type_info[field_name] = _infer_python_type_from_arrow(field.type)
        else:
            # No semantic type metadata - infer from Arrow type
            type_info[field_name] = _infer_python_type_from_arrow(field.type)

    # Convert using reconstructed type info
    _, from_storage = create_packet_converters(type_info, registry)
    storage_packets = table.to_pylist()

    return [from_storage(packet) for packet in storage_packets]


def _infer_python_type_from_arrow(arrow_type: pa.DataType) -> type:
    """Infer Python type from Arrow type as fallback."""
    if arrow_type == pa.int64():
        return int
    elif arrow_type == pa.float64():
        return float
    elif arrow_type == pa.string():
        return str
    elif arrow_type == pa.bool_():
        return bool
    elif arrow_type == pa.binary():
        return bytes
    else:
        return str  # Safe fallback


# TODO: move these functions to util
def escape_with_postfix(field: str, postfix=None, separator="_") -> str:
    """
    Escape the field string by doubling separators and optionally append a postfix.
    This function takes a field string and escapes any occurrences of the separator
    by doubling them, then optionally appends a postfix with a separator prefix.

    Args:
        field (str): The input string containing to be escaped.
        postfix (str, optional): An optional postfix to append to the escaped string.
                               If None, no postfix is added. Defaults to None.
        separator (str, optional): The separator character to escape and use for
                                 prefixing the postfix. Defaults to "_".
    Returns:
        str: The escaped string with optional postfix. Returns empty string if
             fields is provided but postfix is None.
    Examples:
        >>> escape_with_postfix("field1_field2", "suffix")
        'field1__field2_suffix'
        >>> escape_with_postfix("name_age_city", "backup", "_")
        'name__age__city_backup'
        >>> escape_with_postfix("data-info", "temp", "-")
        'data--info-temp'
        >>> escape_with_postfix("simple", None)
        'simple'
        >>> escape_with_postfix("no_separators", "end")
        'no__separators_end'
    """

    return field.replace(separator, separator * 2) + (f"_{postfix}" if postfix else "")


def unescape_with_postfix(field: str, separator="_") -> tuple[str, str | None]:
    """
    Unescape a string by converting double separators back to single separators and extract postfix metadata.
    This function reverses the escaping process where single separators were doubled to avoid
    conflicts with metadata delimiters. It splits the input on double separators, then extracts
    any postfix metadata from the last part.

    Args:
        field (str): The escaped string containing doubled separators and optional postfix metadata
        separator (str, optional): The separator character used for escaping. Defaults to "_"
    Returns:
        tuple[str, str | None]: A tuple containing:
            - The unescaped string with single separators restored
            - The postfix metadata if present, None otherwise
    Examples:
        >>> unescape_with_postfix("field1__field2__field3")
        ('field1_field2_field3', None)
        >>> unescape_with_postfix("field1__field2_metadata")
        ('field1_field2', 'metadata')
        >>> unescape_with_postfix("simple")
        ('simple', None)
        >>> unescape_with_postfix("field1--field2", separator="-")
        ('field1-field2', None)
        >>> unescape_with_postfix("field1--field2-meta", separator="-")
        ('field1-field2', 'meta')
    """

    parts = field.split(separator * 2)
    parts[-1], *meta = parts[-1].split("_", 1)
    return separator.join(parts), meta[0] if meta else None
