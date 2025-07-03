import logging
import pyarrow as pa
from .core import TypeHandler
from dataclasses import dataclass

# This mapping is expected to be stable
# Be sure to test this assumption holds true
DEFAULT_ARROW_TYPE_LUT = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
}

logger = logging.getLogger(__name__)


# TODO: reconsider the need for this dataclass as its information is superfluous
# to the registration of the handler into the registry.
@dataclass
class TypeInfo:
    python_type: type
    storage_type: type
    semantic_type: str | None  # name under which the type is registered
    handler: "TypeHandler"


class SemanticTypeRegistry:
    """Registry that manages type handlers with semantic type names."""

    def __init__(self):
        self._handlers: dict[
            type, tuple[TypeHandler, str]
        ] = {}  # PythonType -> (Handler, semantic_name)
        self._semantic_handlers: dict[str, TypeHandler] = {}  # semantic_name -> Handler
        self._semantic_to_python_lut: dict[
            str, type
        ] = {}  # semantic_name -> Python type

    def register(
        self,
        semantic_type: str,
        handler: TypeHandler,
    ):
        """Register a handler with a semantic type name.

        Args:
            semantic_name: Identifier for this semantic type (e.g., 'path', 'uuid')
            handler: The type handler instance
            explicit_types: Optional override of types to register for (if different from handler's supported_types)
            override: If True, allow overriding existing registration for the same semantic name and Python type(s)
        """
        # Determine which types to register for

        python_type = handler.python_type()

        # Register handler for each type
        if python_type in self._handlers:
            existing_semantic = self._handlers[python_type][1]
            # TODO: handle overlapping registration more gracefully
            raise ValueError(
                f"Type {python_type} already registered with semantic type '{existing_semantic}'"
            )

        # Register by semantic name
        if semantic_type in self._semantic_handlers:
            raise ValueError(f"Semantic type '{semantic_type}' already registered")

        self._handlers[python_type] = (handler, semantic_type)
        self._semantic_handlers[semantic_type] = handler
        self._semantic_to_python_lut[semantic_type] = python_type

    def get_python_type(self, semantic_type: str) -> type | None:
        """Get Python type for a semantic type."""
        return self._semantic_to_python_lut.get(semantic_type)

    def lookup_handler_info(self, python_type: type) -> tuple[TypeHandler, str] | None:
        """Lookup handler info for a Python type."""
        for registered_type, (handler, semantic_type) in self._handlers.items():
            if issubclass(python_type, registered_type):
                return (handler, semantic_type)
        return None

    def get_semantic_type(self, python_type: type) -> str | None:
        """Get semantic type for a Python type."""
        handler_info = self.lookup_handler_info(python_type)
        return handler_info[1] if handler_info else None

    def get_handler(self, python_type: type) -> TypeHandler | None:
        """Get handler for a Python type."""
        handler_info = self.lookup_handler_info(python_type)
        return handler_info[0] if handler_info else None

    def get_handler_by_semantic_type(self, semantic_type: str) -> TypeHandler | None:
        """Get handler by semantic type."""
        return self._semantic_handlers.get(semantic_type)

    def get_type_info(self, python_type: type) -> TypeInfo | None:
        """Get TypeInfo for a Python type."""
        handler = self.get_handler(python_type)
        if handler is None:
            return None
        semantic_type = self.get_semantic_type(python_type)
        return TypeInfo(
            python_type=python_type,
            storage_type=handler.storage_type(),
            semantic_type=semantic_type,
            handler=handler,
        )

    def __contains__(self, python_type: type) -> bool:
        """Check if a Python type is registered."""
        for registered_type in self._handlers:
            if issubclass(python_type, registered_type):
                return True
        return False


# Below is a collection of functions that handles converting between various aspects of Python packets and Arrow tables.
# Here for convenience, any Python dictionary with str keys and supported Python values are referred to as a packet.


# Conversions are:
#  python packet <-> storage packet <-> arrow table
#  python typespec <-> storage typespec <-> arrow schema
#
#  python packet <-> storage packet requires the use of SemanticTypeRegistry
#  conversion between storage packet <-> arrow table requires info about semantic_type


# # Storage packet <-> Arrow table

# def stroage_typespec_to_arrow_schema(storage_typespec:TypeSpec, semantic_type_info: dict[str, str]|None = None) -> pa.Schema:
#     """Convert storage typespec to Arrow Schema with semantic_type metadata."""
#     """Convert storage typespec to PyArrow Schema with semantic_type metadata."""
#     if semantic_type_info is None:
#         semantic_type_info = {}

#     fields = []
#     for field_name, field_type in storage_typespec.items():
#         arrow_type = python_to_pyarrow_type(field_type)
#         semantic_type = semantic_type_info.get(field_name, None)
#         field_metadata = {"semantic_type": semantic_type} if semantic_type else {}
#         fields.append(pa.field(field_name, arrow_type, metadata=field_metadata))
#     return pa.schema(fields)

# def arrow_schema_to_storage_typespec(schema: pa.Schema) -> tuple[TypeSpec, dict[str, str]|None]:
#     """Convert Arrow Schema to storage typespec and semantic type metadata."""
#     typespec = {}
#     semantic_type_info = {}

#     for field in schema:
#         field_type = field.type
#         typespec[field.name] = field_type.to_pandas_dtype()  # Convert Arrow type to Pandas dtype
#         if field.metadata and b"semantic_type" in field.metadata:
#             semantic_type_info[field.name] = field.metadata[b"semantic_type"].decode("utf-8")

#     return typespec, semantic_type_info


# def storage_packet_to_arrow_table(
#     storage_packet: PacketLike,
#     typespec: TypeSpec | None = None,
#     semantic_type_info: dict[str, str] | None = None,


# # TypeSpec + TypeRegistry + ArrowLUT -> Arrow Schema (annotated with semantic_type)

# #


# # TypeSpec <-> Arrow Schema

# def schema_from_typespec(typespec: TypeSpec, registry: SemanticTypeRegistry, metadata_info: dict | None = None) -> pa.Schema:
#     """Convert TypeSpec to PyArrow Schema."""
#     if metadata_info is None:
#         metadata_info = {}

#     fields = []
#     for field_name, field_type in typespec.items():
#         type_info = registry.get_type_info(field_type)
#         if type_info is None:
#             raise ValueError(f"No type info registered for {field_type}")
#         fields.append(pa.field(field_name, type_info.arrow_type, metadata={
#             "semantic_type": type_info.semantic_type
#         }))
#     return pa.schema(fields)

# def create_schema_from_typespec(
#     typespec: TypeSpec,
#     registry: SemanticTypeRegistry,
#     metadata_info: dict | None = None,
#     arrow_type_lut: dict[type, pa.DataType] | None = None,
# ) -> tuple[list[tuple[str, TypeHandler]], pa.Schema]:
#     if metadata_info is None:
#         metadata_info = {}
#     if arrow_type_lut is None:
#         arrow_type_lut = DEFAULT_ARROW_TYPE_LUT

#     keys_with_handlers: list[tuple[str, TypeHandler]] = []
#     schema_fields = []
#     for key, python_type in typespec.items():
#         type_info = registry.get_type_info(python_type)

#         field_metadata = {}
#         if type_info and type_info.semantic_type:
#             field_metadata["semantic_type"] = type_info.semantic_type
#             keys_with_handlers.append((key, type_info.handler))
#             arrow_type = type_info.arrow_type
#         else:
#             arrow_type = arrow_type_lut.get(python_type)
#             if arrow_type is None:
#                 raise ValueError(
#                     f"Direct support for Python type {python_type} is not provided. Register a handler to work with {python_type}"
#                 )

#         schema_fields.append(pa.field(key, arrow_type, metadata=field_metadata))
#     return keys_with_handlers, pa.schema(schema_fields)


# def arrow_table_to_packets(
#     table: pa.Table,
#     registry: SemanticTypeRegistry,
# ) -> list[Packet]:
#     """Convert Arrow table to packet with field metadata.

#     Args:
#         packet: Dictionary mapping parameter names to Python values

#     Returns:
#         PyArrow Table with the packet data as a single row
#     """
#     packets: list[Packet] = []

#     # prepare converter for each field

#     def no_op(x) -> Any:
#         return x

#     converter_lut = {}
#     for field in table.schema:
#         if field.metadata and b"semantic_type" in field.metadata:
#             semantic_type = field.metadata[b"semantic_type"].decode("utf-8")
#             if semantic_type:
#                 handler = registry.get_handler_by_semantic_name(semantic_type)
#                 if handler is None:
#                     raise ValueError(
#                         f"No handler registered for semantic type '{semantic_type}'"
#                     )
#                 converter_lut[field.name] = handler.storage_to_python

#     # Create packets from the Arrow table
#     # TODO: make this more efficient
#     for row in range(table.num_rows):
#         packet: Packet = Packet()
#         for field in table.schema:
#             value = table.column(field.name)[row].as_py()
#             packet[field.name] = converter_lut.get(field.name, no_op)(value)
#         packets.append(packet)

#     return packets


# def create_arrow_table_with_meta(
#     storage_packet: dict[str, Any], type_info: dict[str, TypeInfo]
# ):
#     """Create an Arrow table with metadata from a storage packet.

#     Args:
#         storage_packet: Dictionary with values in storage format
#         type_info: Dictionary mapping parameter names to TypeInfo objects

#     Returns:
#         PyArrow Table with metadata
#     """
#     schema_fields = []
#     for key, type_info_obj in type_info.items():
#         field_metadata = {}
#         if type_info_obj.semantic_type:
#             field_metadata["semantic_type"] = type_info_obj.semantic_type

#         field = pa.field(key, type_info_obj.arrow_type, metadata=field_metadata)
#         schema_fields.append(field)

#     schema = pa.schema(schema_fields)

#     arrays = []
#     for field in schema:
#         value = storage_packet[field.name]
#         array = pa.array([value], type=field.type)
#         arrays.append(array)

#     return pa.Table.from_arrays(arrays, schema=schema)


# def retrieve_storage_packet_from_arrow_with_meta(
#     arrow_table: pa.Table,
# ) -> dict[str, Any]:
#     """Retrieve storage packet from Arrow table with metadata.

#     Args:
#         arrow_table: PyArrow Table with metadata

#     Returns:
#         Dictionary representing the storage packet
#     """
#     storage_packet = {}
#     for field in arrow_table.schema:
#         # Extract value from Arrow array
#         array = arrow_table.column(field.name)
#         if array.num_chunks > 0:
#             value = array.chunk(0).as_py()[0]  # Get first value
#         else:
#             value = None  # Handle empty arrays

#         storage_packet[field.name] = value

#     return storage_packet

# def typespec_to_schema_with_metadata(typespec: TypeSpec, field_metadata: dict|None = None) -> pa.Schema:
#     """Convert TypeSpec to PyArrow Schema"""
#     fields = []
#     for field_name, field_type in typespec.items():
#         arrow_type = python_to_pyarrow_type(field_type)
#         fields.append(pa.field(field_name, arrow_type))
#     return pa.schema(fields)

# def python_to_pyarrow_type(python_type: type, strict:bool=True) -> pa.DataType:
#     """Convert Python type (including generics) to PyArrow type"""
#     # For anywhere we need to store str value, we use large_string as is done in Polars

#     # Handle basic types first
#     basic_mapping = {
#         int: pa.int64(),
#         float: pa.float64(),
#         str: pa.large_string(),
#         bool: pa.bool_(),
#         bytes: pa.binary(),
#     }

#     if python_type in basic_mapping:
#         return basic_mapping[python_type]

#     # Handle generic types
#     origin = get_origin(python_type)
#     args = get_args(python_type)

#     if origin is list:
#         # Handle list[T]
#         if args:
#             element_type = python_to_pyarrow_type(args[0])
#             return pa.list_(element_type)
#         else:
#             return pa.list_(pa.large_string())  # default to list of strings

#     elif origin is dict:
#         # Handle dict[K, V] - PyArrow uses map type
#         if len(args) == 2:
#             key_type = python_to_pyarrow_type(args[0])
#             value_type = python_to_pyarrow_type(args[1])
#             return pa.map_(key_type, value_type)
#         else:
#             # Otherwise default to using long string
#             return pa.map_(pa.large_string(), pa.large_string())

#     elif origin is UnionType:
#         # Handle Optional[T] (Union[T, None])
#         if len(args) == 2 and type(None) in args:
#             non_none_type = args[0] if args[1] is type(None) else args[1]
#             return python_to_pyarrow_type(non_none_type)

#     # Default fallback
#     if not strict:
#         logger.warning(f"Unsupported type {python_type}, defaulting to large_string")
#         return pa.large_string()
#     else:
#         raise TypeError(f"Unsupported type {python_type} for PyArrow conversion. "
#                         "Set strict=False to allow fallback to large_string.")

# def arrow_to_dicts(table: pa.Table) -> list[dict[str, Any]]:
#     """
#     Convert Arrow table to dictionary or list of dictionaries.
#     Returns a list of dictionaries (one per row) with column names as keys.
#     Args:
#         table: PyArrow Table to convert
#     Returns:
#         A list of dictionaries for multi-row tables.
#     """
#     if len(table) == 0:
#         return []

#     # Multiple rows: return list of dicts (one per row)
#     return [
#         {col_name: table.column(col_name)[i].as_py() for col_name in table.column_names}
#         for i in range(len(table))
#     ]

# def get_metadata_from_schema(
#     schema: pa.Schema, metadata_field: bytes
# ) -> dict[str, str]:
#     """
#     Extract metadata from Arrow schema fields. Metadata value will be utf-8 decoded.
#     Args:
#         schema: PyArrow Schema to extract metadata from
#         metadata_field: Metadata field to extract (e.g., b'semantic_type')
#     Returns:
#         Dictionary mapping field names to their metadata values
#     """
#     metadata = {}
#     for field in schema:
#         if field.metadata and metadata_field in field.metadata:
#             metadata[field.name] = field.metadata[metadata_field].decode("utf-8")
#     return metadata

# def dict_to_arrow_table_with_metadata(data: dict, data_type_info: TypeSpec | None = None, metadata: dict | None = None):
#     """
#     Convert a tag dictionary to PyArrow table with metadata on each column.

#     Args:
#         tag: Dictionary with string keys and any Python data type values
#         metadata_key: The metadata key to add to each column
#         metadata_value: The metadata value to indicate this column came from tag
#     """
#     if metadata is None:
#         metadata = {}

#     if field_types is None:
#         # First create the table to infer types
#         temp_table = pa.Table.from_pylist([data])

#     # Create new fields with metadata
#     fields_with_metadata = []
#     for field in temp_table.schema:
#         # Add metadata to each field
#         field_metadata = metadata
#         new_field = pa.field(
#             field.name, field.type, nullable=field.nullable, metadata=field_metadata
#         )
#         fields_with_metadata.append(new_field)

#     # Create schema with metadata
#     schema_with_metadata = pa.schema(fields_with_metadata)

#     # Create the final table with the metadata-enriched schema
#     table = pa.Table.from_pylist([tag], schema=schema_with_metadata)

#     return table


# # def get_columns_with_metadata(
# #     df: pl.DataFrame, key: str, value: str | None = None
# # ) -> list[str]:
# #     """Get column names with specific metadata using list comprehension. If value is given, only
# #     columns matching that specific value for the desginated metadata key will be returned.
# #     Otherwise, all columns that contains the key as metadata will be returned regardless of the value"""
# #     return [
# #         col_name
# #         for col_name, dtype in df.schema.items()
# #         if hasattr(dtype, "metadata")
# #         and (value is None or getattr(dtype, "metadata") == value)
# #     ]
