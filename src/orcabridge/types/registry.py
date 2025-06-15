from collections.abc import Callable, Collection, Sequence
import logging
from optparse import Values
from typing import Any
import pyarrow as pa
from orcabridge.types import Packet
from .core import TypeHandler, TypeInfo, TypeSpec

# This mapping is expected to be stable
# Be sure to test this assumption holds true
DEFAULT_ARROW_TYPE_LUT = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
}

logger = logging.getLogger(__name__)


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
            supported = handler.python_types()
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

    def get_type_info(self, python_type: type) -> TypeInfo | None:
        """Get TypeInfo for a Python type."""
        handler = self.get_handler(python_type)
        if handler is None:
            return None
        semantic_name = self.get_semantic_name(python_type)
        return TypeInfo(
            python_type=python_type,
            arrow_type=handler.storage_type(),
            semantic_type=semantic_name,
            handler=handler,
        )

    def get_handler_by_semantic_name(self, semantic_name: str) -> TypeHandler | None:
        """Get handler by semantic name."""
        return self._semantic_handlers.get(semantic_name)

    def __contains__(self, python_type: type) -> bool:
        """Check if a Python type is registered."""
        return python_type in self._handlers


class PacketConverter:
    def __init__(self, python_type_spec: TypeSpec, registry: TypeRegistry):
        self.python_type_spec = python_type_spec
        self.registry = registry

        # Lookup handlers and type info for fast access
        self.handlers: dict[str, TypeHandler] = {}
        self.storage_type_info: dict[str, TypeInfo] = {}

        self.expected_key_set = set(python_type_spec.keys())

        # prepare the corresponding arrow table schema with metadata
        self.keys_with_handlers, self.schema = create_schema_from_python_type_info(
            python_type_spec, registry
        )

        self.semantic_type_lut = get_metadata_from_schema(self.schema, b"semantic_type")

    def _check_key_consistency(self, keys):
        """Check if the provided keys match the expected keys."""
        keys_set = set(keys)
        if keys_set != self.expected_key_set:
            missing_keys = self.expected_key_set - keys_set
            extra_keys = keys_set - self.expected_key_set
            error_parts = []
            if missing_keys:
                error_parts.append(f"Missing keys: {missing_keys}")
            if extra_keys:
                error_parts.append(f"Extra keys: {extra_keys}")

            raise KeyError(f"Keys don't match expected keys. {'; '.join(error_parts)}")

    def _to_storage_packet(self, packet: Packet) -> dict[str, Any]:
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

        self._check_key_consistency(packet_keys)

        # Convert each value
        storage_packet: dict[str, Any] = (
            packet.copy()
        )  # Start with a copy of the packet

        for key, handler in self.keys_with_handlers:
            try:
                storage_packet[key] = handler.python_to_storage(storage_packet[key])
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e

        return storage_packet

    def _from_storage_packet(self, storage_packet: dict[str, Any]) -> Packet:
        """Convert storage packet back to Python packet.

        Args:
            storage_packet: Dictionary with values in storage format

        Returns:
            Packet with values converted back to Python types

        Raises:
            KeyError: If storage packet keys don't match the expected type_info keys
            TypeError: If value type doesn't match expected type
            ValueError: If conversion fails
        """
        # Validate storage packet keys
        storage_keys = set(storage_packet.keys())

        self._check_key_consistency(storage_keys)

        # Convert each value back to Python type
        packet: Packet = storage_packet.copy()

        for key, handler in self.keys_with_handlers:
            try:
                packet[key] = handler.storage_to_python(storage_packet[key])
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e

        return packet

    def to_arrow_table(self, packet: Packet | Sequence[Packet]) -> pa.Table:
        """Convert packet to PyArrow Table with field metadata.

        Args:
            packet: Dictionary mapping parameter names to Python values

        Returns:
            PyArrow Table with the packet data as a single row
        """
        # Convert packet to storage format
        if not isinstance(packet, Sequence):
            packets = [packet]
        else:
            packets = packet

        storage_packets = [self._to_storage_packet(p) for p in packets]

        # Create arrays
        arrays = []
        for field in self.schema:
            values = [p[field.name] for p in storage_packets]
            array = pa.array(values, type=field.type)
            arrays.append(array)

        return pa.Table.from_arrays(arrays, schema=self.schema)

    def from_arrow_table(
        self, table: pa.Table, verify_semantic_equivalence: bool = True
    ) -> list[Packet]:
        """Convert Arrow table to packet with field metadata.

        Args:
            table: PyArrow Table with metadata

        Returns:
            List of packets converted from the Arrow table
        """
        # Check for consistency in the semantic type mapping:
        semantic_type_info = get_metadata_from_schema(table.schema, b"semantic_type")

        if semantic_type_info != self.semantic_type_lut:
            if not verify_semantic_equivalence:
                logger.warning(
                    "Arrow table semantic types do not match expected type registry. "
                    f"Expected: {self.semantic_type_lut}, got: {semantic_type_info}"
                )
            else:
                raise ValueError(
                    "Arrow table semantic types do not match expected type registry. "
                    f"Expected: {self.semantic_type_lut}, got: {semantic_type_info}"
                )

        # Create packets from the Arrow table
        # TODO: make this more efficient
        storage_packets: list[Packet] = arrow_to_dicts(table)  # type: ignore
        if not self.keys_with_handlers:
            # no special handling required
            return storage_packets

        return [self._from_storage_packet(packet) for packet in storage_packets]


def arrow_to_dicts(table: pa.Table) -> list[dict[str, Any]]:
    """
    Convert Arrow table to dictionary or list of dictionaries.
    By default returns a list of dictionaries (one per row) with column names as keys.
    If `collapse_singleton` is True, return a single dictionary for single-row tables.
    Args:
        table: PyArrow Table to convert
        collapse_singleton: If True, return a single dictionary for single-row tables. Defaults to False.
    Returns:
        A dictionary if singleton and collapse_singleton=True. Otherwise, list of dictionaries for multi-row tables.
    """
    if len(table) == 0:
        return []

    # Multiple rows: return list of dicts (one per row)
    return [
        {col_name: table.column(col_name)[i].as_py() for col_name in table.column_names}
        for i in range(len(table))
    ]


def get_metadata_from_schema(
    schema: pa.Schema, metadata_field: bytes
) -> dict[str, str]:
    """
    Extract metadata from Arrow schema fields. Metadata value will be utf-8 decoded.
    Args:
        schema: PyArrow Schema to extract metadata from
        metadata_field: Metadata field to extract (e.g., b'semantic_type')
    Returns:
        Dictionary mapping field names to their metadata values
    """
    metadata = {}
    for field in schema:
        if field.metadata and metadata_field in field.metadata:
            metadata[field.name] = field.metadata[metadata_field].decode("utf-8")
    return metadata


def create_schema_from_python_type_info(
    python_type_spec: TypeSpec,
    registry: TypeRegistry,
    arrow_type_lut: dict[type, pa.DataType] | None = None,
) -> tuple[list[tuple[str, TypeHandler]], pa.Schema]:
    if arrow_type_lut is None:
        arrow_type_lut = DEFAULT_ARROW_TYPE_LUT
    keys_with_handlers: list[tuple[str, TypeHandler]] = []
    schema_fields = []
    for key, python_type in python_type_spec.items():
        type_info = registry.get_type_info(python_type)

        field_metadata = {}
        if type_info and type_info.semantic_type:
            field_metadata["semantic_type"] = type_info.semantic_type
            keys_with_handlers.append((key, type_info.handler))
            arrow_type = type_info.arrow_type
        else:
            arrow_type = arrow_type_lut.get(python_type)
            if arrow_type is None:
                raise ValueError(
                    f"Direct support for Python type {python_type} is not provided. Register a handler to work with {python_type}"
                )

        schema_fields.append(pa.field(key, arrow_type, metadata=field_metadata))
    return keys_with_handlers, pa.schema(schema_fields)


def arrow_table_to_packets(
    table: pa.Table,
    registry: TypeRegistry,
) -> list[Packet]:
    """Convert Arrow table to packet with field metadata.

    Args:
        packet: Dictionary mapping parameter names to Python values

    Returns:
        PyArrow Table with the packet data as a single row
    """
    packets: list[Packet] = []

    # prepare converter for each field

    def no_op(x) -> Any:
        return x

    converter_lut = {}
    for field in table.schema:
        if field.metadata and b"semantic_type" in field.metadata:
            semantic_type = field.metadata[b"semantic_type"].decode("utf-8")
            if semantic_type:
                handler = registry.get_handler_by_semantic_name(semantic_type)
                if handler is None:
                    raise ValueError(
                        f"No handler registered for semantic type '{semantic_type}'"
                    )
                converter_lut[field.name] = handler.storage_to_python

    # Create packets from the Arrow table
    # TODO: make this more efficient
    for row in range(table.num_rows):
        packet: Packet = {}
        for field in table.schema:
            value = table.column(field.name)[row].as_py()
            packet[field.name] = converter_lut.get(field.name, no_op)(value)
        packets.append(packet)

    return packets


def is_packet_supported(
    python_type_info: TypeSpec, registry: TypeRegistry, type_lut: dict | None = None
) -> bool:
    """Check if all types in the packet are supported by the registry or known to the default lut."""
    if type_lut is None:
        type_lut = {}
    return all(
        python_type in registry or python_type in type_lut
        for python_type in python_type_info.values()
    )


def create_arrow_table_with_meta(
    storage_packet: dict[str, Any], type_info: dict[str, TypeInfo]
):
    """Create an Arrow table with metadata from a storage packet.

    Args:
        storage_packet: Dictionary with values in storage format
        type_info: Dictionary mapping parameter names to TypeInfo objects

    Returns:
        PyArrow Table with metadata
    """
    schema_fields = []
    for key, type_info_obj in type_info.items():
        field_metadata = {}
        if type_info_obj.semantic_type:
            field_metadata["semantic_type"] = type_info_obj.semantic_type

        field = pa.field(key, type_info_obj.arrow_type, metadata=field_metadata)
        schema_fields.append(field)

    schema = pa.schema(schema_fields)

    arrays = []
    for field in schema:
        value = storage_packet[field.name]
        array = pa.array([value], type=field.type)
        arrays.append(array)

    return pa.Table.from_arrays(arrays, schema=schema)


def retrieve_storage_packet_from_arrow_with_meta(
    arrow_table: pa.Table,
) -> dict[str, Any]:
    """Retrieve storage packet from Arrow table with metadata.

    Args:
        arrow_table: PyArrow Table with metadata

    Returns:
        Dictionary representing the storage packet
    """
    storage_packet = {}
    for field in arrow_table.schema:
        # Extract value from Arrow array
        array = arrow_table.column(field.name)
        if array.num_chunks > 0:
            value = array.chunk(0).as_py()[0]  # Get first value
        else:
            value = None  # Handle empty arrays

        storage_packet[field.name] = value

    return storage_packet
