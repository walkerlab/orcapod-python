"""
Data structures and utilities for working with datagrams in OrcaPod.

This module provides classes and functions for handling packet-like data structures
that can represent data in various formats (Python dicts, Arrow tables, etc.) while
maintaining type information, source metadata, and semantic type conversion capability.

Key classes:
- SemanticConverter: Converts between different data representations. Intended for internal use.
- DictDatagram: Immutable dict-based data structure
- PythonDictPacket: Python dict-based packet with source info
- ArrowPacket: Arrow table-based packet implementation
- PythonDictTag/ArrowTag: Tag implementations for data identification

The module also provides utilities for schema validation, table operations,
and type conversions between semantic stores, Python stores, and Arrow tables.
"""

from orcapod.types.core import DataValue
from typing import TypeAlias, Self
from collections.abc import Mapping, Collection
from orcapod.types import TypeSpec
from orcapod.types.semantic_converter import SemanticConverter
from orcapod.protocols import data_protocols as dp, hashing_protocols as hp
from orcapod.types.semantic_types import SemanticTypeRegistry
from orcapod.types import schemas
from orcapod.types import typespec_utils as tsutils
import pyarrow as pa
import logging
from orcapod.utils import arrow_utils


# Constants used for source info keys
SOURCE_INFO_PREFIX = "_source_info_"


logger = logging.getLogger(__name__)
# A conveniece packet-like type that defines a value that can be
# converted to a packet. It's broader than Packet and a simple mapping
# from string keys to DataValue (e.g., int, float, str) can be regarded
# as PacketLike, allowing for more flexible interfaces.
# Anything that requires Packet-like data but without the strict features
# of a Packet should accept PacketLike.
# One should be careful when using PacketLike as a return type as it does not
# enforce the typespec or source_info, which are important for packet integrity.
PacketLike: TypeAlias = Mapping[str, DataValue]

PythonStore: TypeAlias = Mapping[str, DataValue]


# class SemanticConverter:
#     """
#     Converts data between different representations (Python, semantic stores, Arrow tables).

#     SemanticConverter only tracks the semantic columns to be converted and does not
#     enforce any type checking on other columns. Consequently, two completely different
#     schemas could share a semantic converter if the have same named fields with identical
#     semantic types. Furthermore, semantic types are defined by the association of semantic
#     type name with a specific TypeHandler.

#     """

#     @staticmethod
#     def prepare_handler(
#         semantic_schema: schemas.SemanticSchema,
#         semantic_type_registry: SemanticTypeRegistry,
#     ) -> dict[str, TypeHandler]:
#         """
#         Prepare type handlers for semantic type conversion.

#         Args:
#             semantic_schema: Schema containing semantic type information
#             semantic_type_registry: Registry for looking up type handlers

#         Returns:
#             Dictionary mapping field names to their type handlers
#         """
#         handler_lut = {}
#         for key, (_, semantic_type) in semantic_schema.items():
#             if semantic_type is None:
#                 continue  # Skip keys without semantic type
#             handler_lut[key] = semantic_type_registry.get_handler_by_semantic_type(
#                 semantic_type
#             )
#         return handler_lut

#     @classmethod
#     def from_typespec(
#         cls, typespec: TypeSpec, semantic_type_registry: SemanticTypeRegistry
#     ) -> "SemanticConverter":
#         """
#         Create a SemanticConverter from a basic Python type specification dictionary (TypeSpec).

#         Args:
#             typespec: Type specification dictionary
#             semantic_type_registry: Registry for semantic type lookup

#         Returns:
#             New SemanticConverter instance
#         """
#         semantic_schema = schemas.from_typespec_to_semantic_schema(
#             typespec, semantic_type_registry
#         )
#         python_schema = schemas.PythonSchema(typespec)
#         handler_lut = cls.prepare_handler(semantic_schema, semantic_type_registry)
#         return cls(python_schema, semantic_schema, handler_lut)

#     @classmethod
#     def from_arrow_schema(
#         cls, arrow_schema: pa.Schema, semantic_type_registry: SemanticTypeRegistry
#     ) -> "SemanticConverter":
#         """
#         Create a SemanticConverter from an Arrow schema.

#         Args:
#             arrow_schema: PyArrow schema with semantic type metadata
#             semantic_type_registry: Registry for semantic type lookup

#         Returns:
#             New SemanticConverter instance
#         """
#         semantic_schema = schemas.from_arrow_schema_to_semantic_schema(arrow_schema)
#         python_schema = schemas.from_semantic_schema_to_python_schema(
#             semantic_schema, semantic_type_registry=semantic_type_registry
#         )
#         handler_lut = cls.prepare_handler(semantic_schema, semantic_type_registry)
#         return cls(python_schema, semantic_schema, handler_lut)

#     def __init__(
#         self,
#         handler_lut: dict[str, tuple[str, TypeHandler]] | None = None,
#     ):
#         """
#         Initialize SemanticConverter with schemas and type handlers. This is not meant to be called directly.
#         Use class methods like `from_arrow_schema` or `from_typespec` instead.

#         Args:
#             python_schema: Schema for Python data types
#             semantic_schema: Schema for semantic types
#             handler_lut: Optional dictionary of type handlers for conversion
#         """
#         if handler_lut is None:
#             handler_lut = {}
#         self.handler_lut = handler_lut

#     def convert_from_semantic_to_python(
#         self, semantic_value: Any, semantic_type: SemanticType
#     ) -> Any:
#         """
#         Convert a semantic value to a Python value.

#         Args:
#             semantic_value: Value in semantic (storage-optimized) format
#             semantic_type: Corresponding semantic type

#         Returns:
#             Value in Python native format
#         """
#         handler = self.handler_lut.get(semantic_type)
#         if handler:
#             return handler.to_canonical(semantic_value)
#         return semantic_value

#     def from_semantic_store_to_python_store(
#         self, semantic_store: SemanticStore
#     ) -> dict[str, DataValue]:
#         """
#         Convert a semantic store to a Python store.

#         Args:
#             semantic_store: Store (dict) with data stored in semantic (storage-optimized) types

#         Returns:
#             Store with Python native types
#         """
#         python_store = dict(semantic_store)
#         for key, handler in self.handler_lut.items():
#             python_store[key] = handler.storage_to_python(semantic_store[key])
#         # TODO: come up with a more robust handling/conversion
#         return cast(dict[str, DataValue], python_store)

#     def from_python_store_to_semantic_store(
#         self, python_store: PythonStore
#     ) -> SemanticStore:
#         """
#         Convert a Python store to a semantic store.

#         Args:
#             python_store: Store with Python native types

#         Returns:
#             Store with semantic (storage-optimized) types
#         """
#         semantic_store = dict(python_store)
#         for key, handler in self.handler_lut.items():
#             semantic_store[key] = handler.python_to_storage(python_store[key])
#         return semantic_store  # type: ignore[return-value]

#     def from_semantic_store_to_arrow_table(
#         self, semantic_store: SemanticStore
#     ) -> pa.Table:
#         """Convert a semantic store to an Arrow table."""
#         return pa.Table.from_pylist([semantic_store], schema=self.arrow_schema)

#     def from_python_store_to_arrow_table(self, python_store: PythonStore) -> pa.Table:
#         """Convert a Python store to an Arrow table."""
#         semantic_store = self.from_python_store_to_semantic_store(python_store)
#         return self.from_semantic_store_to_arrow_table(semantic_store)

#     def from_arrow_table_to_semantic_stores(
#         self, arrow_table: pa.Table
#     ) -> list[SemanticStore]:
#         """Convert an Arrow table to a list of semantic stores."""
#         self.verify_compatible_arrow_schema(arrow_table.schema)
#         return arrow_table.to_pylist()  # Ensure the table is materialized

#     def from_arrow_table_to_python_stores(
#         self, arrow_table: pa.Table
#     ) -> list[dict[str, DataValue]]:
#         """Convert an Arrow table to a list of Python stores."""
#         return [
#             self.from_semantic_store_to_python_store(semantic_store)
#             for semantic_store in self.from_arrow_table_to_semantic_stores(arrow_table)
#         ]

#     def verify_compatible_arrow_schema(self, arrow_schema: pa.Schema):
#         """
#         Verify that an Arrow schema is compatible with the expected schema.

#         Args:
#             arrow_schema: Schema to verify

#         Raises:
#             ValueError: If schemas are incompatible
#         """
#         compatible, errors = check_arrow_schema_compatibility(
#             arrow_schema, self.arrow_schema
#         )
#         if not compatible:
#             raise ValueError(
#                 "Arrow table schema is not compatible with the expected schema: "
#                 + ", ".join(errors)
#             )


class ImmutableDict(Mapping[str, DataValue]):
    """
    An immutable dictionary-like container for DataValues.

    Provides a read-only view of a dictionary mapping strings to DataValues,
    implementing the Mapping protocol for compatibility with dict-like operations.

    Initialize with data from a mapping.
    Args:
        data: Source mapping to copy data from
    """

    def __init__(self, data: Mapping[str, DataValue]):
        self._data = dict(data)

    def __getitem__(self, key: str) -> DataValue:
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return self._data.__repr__()

    def __str__(self) -> str:
        return self._data.__str__()


# TODO: Inherit from Mapping instead to provide immutable datagram
class DictDatagram(ImmutableDict):
    """
    An immutable datagram implementation using a dictionary backend.

    Extends ImmutableDict to provide additional functionality for type handling,
    semantic conversion, and Arrow table representation while maintaining
    immutability of the underlying data.


    Initialize DictDatagram with data and optional type information.

    Args:
        data: Source data mapping
        typespec: Optional type specification for fields
        semantic_converter: Optional converter for semantic types
        semantic_type_registry: Registry for semantic type lookup
        arrow_hasher: Optional hasher for Arrow table content
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        typespec: TypeSpec | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
    ) -> None:
        # normalize the data content and remove any source info keys
        super().__init__(data)

        # combine provided typespec info with inferred typespec from content
        verified_typespec = {}
        if typespec is not None:
            verified_typespec = dict(typespec)
        # TODO: enhance get_typespec_from_dict to also use info from supplied typespec dict
        inferred_typespec = tsutils.get_typespec_from_dict(self)
        for key in self:
            if key not in verified_typespec:
                verified_typespec[key] = inferred_typespec[key]
        self._python_schema = schemas.PythonSchema(verified_typespec)

        # create semantic converter
        if semantic_converter is None:
            semantic_converter = SemanticConverter.from_semantic_schema(
                self._python_schema.to_semantic_schema(
                    semantic_type_registry=semantic_type_registry
                ),
            )
        self.semantic_converter = semantic_converter

        self._arrow_hasher = arrow_hasher

        self._cached_table: pa.Table | None = None
        self._cached_content_hash: str | None = None

    def as_table(
        self,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""

        if self._cached_table is None:
            self._cached_table = self.semantic_converter.from_python_to_arrow(
                self, self.types()
            )
            assert self._cached_table is not None, "Cached table should not be None"
        return self._cached_table

    def as_dict(self) -> dict[str, DataValue]:
        """Return dictionary representation of the datagram."""
        return dict(self)

    def content_hash(
        self,
    ) -> str:
        """
        Calculate and return content hash of the datagram.

        Returns:
            Hash string of the datagram content
        """
        if self._cached_content_hash is None:
            if self._arrow_hasher is None:
                raise ValueError(
                    "Arrow hasher must be provided to calculate content hash."
                )
            self._cached_content_hash = self._arrow_hasher.hash_table(
                self.as_table(),
                prefix_hasher_id=True,
            )
        return self._cached_content_hash

    # use keys() implementation from dict

    def types(self) -> schemas.PythonSchema:
        """Return copy of the Python schema."""
        return self._python_schema.copy()

    @classmethod
    def _from_copy(
        cls,
        data: Mapping[str, DataValue],
        python_schema: schemas.PythonSchema,
        semantic_converter: SemanticConverter,
        arrow_hasher: hp.ArrowHasher | None,
    ) -> Self:
        """Create a new instance from copy without full initialization."""
        instance = cls.__new__(cls)
        ImmutableDict.__init__(instance, data)

        # Set attributes directly
        instance._python_schema = python_schema
        instance.semantic_converter = semantic_converter
        instance._arrow_hasher = arrow_hasher
        instance._cached_table = None
        instance._cached_content_hash = None

        return instance

    def copy(self) -> Self:
        """Return a copy of the datagram."""
        return self._from_copy(
            self,
            self._python_schema.copy(),
            self.semantic_converter,
            self._arrow_hasher,
        )


class DictTag(DictDatagram):
    """
    A simple tag implementation using Python dictionary.

    Represents a tag (metadata) as a dictionary that can be converted
    to different representations like Arrow tables.
    """


class DictPacket(DictDatagram):
    """
    Enhanced packet implementation with source information support.

    Extends DictDatagram to include source information tracking and
    enhanced table conversion capabilities that can include or exclude
    source metadata.

    Initialize packet with data and optional source information.

    Args:
        data: Primary data content
        source_info: Optional mapping of field names to source information
        typespec: Optional type specification
        semantic_converter: Optional semantic converter
        semantic_type_registry: Registry for semantic types. Defaults to system default registry.
        arrow_hasher: Optional Arrow hasher. Defaults to system default arrow hasher.
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        source_info: Mapping[str, str | None] | None = None,
        typespec: TypeSpec | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
    ) -> None:
        # normalize the data content and remove any source info keys
        data_only = {
            k: v for k, v in data.items() if not k.startswith(SOURCE_INFO_PREFIX)
        }
        contained_source_info = {
            k.removeprefix(SOURCE_INFO_PREFIX): v
            for k, v in data.items()
            if k.startswith(SOURCE_INFO_PREFIX)
        }

        super().__init__(
            data_only,
            typespec=typespec,
            semantic_converter=semantic_converter,
            semantic_type_registry=semantic_type_registry,
            arrow_hasher=arrow_hasher,
        )

        self._source_info = {**contained_source_info, **(source_info or {})}
        self._cached_source_info_table: pa.Table | None = None

    def as_table(
        self,
        include_source: bool = False,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        table = super().as_table()
        if include_source:
            if self._cached_source_info_table is None:
                source_info_data = {
                    f"{SOURCE_INFO_PREFIX}{k}": v for k, v in self.source_info().items()
                }
                source_info_schema = pa.schema(
                    {k: pa.large_string() for k in source_info_data}
                )
                self._cached_source_info_table = pa.Table.from_pylist(
                    [source_info_data], schema=source_info_schema
                )
            assert self._cached_source_info_table is not None, (
                "Cached source info table should not be None"
            )
            # subselect the corresponding _source_info as the columns present in the data table
            source_info_table = self._cached_source_info_table.select(
                [f"{SOURCE_INFO_PREFIX}{k}" for k in table.column_names]
            )
            table = arrow_utils.hstack_tables(table, source_info_table)
        return table

    def as_dict(self, include_source: bool = False) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = dict(self)
        if include_source:
            for key, value in self.source_info().items():
                dict_copy[f"{SOURCE_INFO_PREFIX}{key}"] = value
        return dict_copy

    def as_datagram(self, include_source: bool = False) -> DictDatagram:
        """
        Convert the packet to a DictDatagram.

        Args:
            include_source: Whether to include source info fields

        Returns:
            DictDatagram representation of the packet
        """
        data = self.as_dict(include_source=include_source)
        typespec = self.types()
        # append source info to typespec if requested
        if include_source:
            for key in self.keys():
                typespec[f"{SOURCE_INFO_PREFIX}{key}"] = str
        return DictDatagram(
            data,
            typespec=typespec,
            semantic_converter=self.semantic_converter,
            arrow_hasher=self._arrow_hasher,
        )

    # def content_hash2(self) -> str:
    #     """
    #     Calculate content hash excluding source information.

    #     Returns:
    #         Hash string of the packet content
    #     """
    #     # TODO: check if this is identical to DictDatagram.content_hash
    #     if self._cached_content_hash is None:
    #         self._cached_content_hash = self._arrow_hasher.hash_table(
    #             self.as_table(include_source=False), prefix_hasher_id=True
    #         )
    #     return self._cached_content_hash

    # use keys() implementation from dict

    def types(self) -> schemas.PythonSchema:
        """
        Returns:
            Packet type information as PythonSchema (dict mapping field names to types).
        """
        return self._python_schema.copy()

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Dictionary mapping field names to their source info
        """
        return {key: self._source_info.get(key, None) for key in self.keys()}

    def copy(self) -> Self:
        """Return a shallow copy of the packet."""
        instance = super().copy()
        instance._source_info = self._source_info.copy()
        instance._cached_source_info_table = self._cached_source_info_table
        return instance


def prepare_data_and_source_tables(
    table: pa.Table, source_info: dict[str, str | None] | None = None
) -> tuple[pa.Table, pa.Table]:
    """
    Process a table to ensure proper source_info columns.

    Args:
        table: Input PyArrow table
        source_info: optional dictionary mapping column names to source info values. If present,
                     it will take precedence over existing source_info columns in the table.

    Returns:
        tuple of table without any source info and another table only containing source info columns (with prefix)
    """
    if source_info is None:
        source_info = {}

    # Step 1: Separate source_info columns from regular columns
    data_columns = []
    data_column_names = []
    existing_source_info = {}

    for i, name in enumerate(table.column_names):
        if name.startswith(SOURCE_INFO_PREFIX):
            # Extract the base column name
            base_name = name.removeprefix(SOURCE_INFO_PREFIX)
            existing_source_info[base_name] = table.column(i)
        else:
            data_columns.append(table.column(i))
            data_column_names.append(name)

    # Step 2: Create source_info columns for each regular column
    source_info_columns = []
    source_info_column_names = []

    # Add all regular columns first

    # Create source_info columns for each regular column
    num_rows = table.num_rows

    for col_name in data_column_names:
        source_info_col_name = f"{SOURCE_INFO_PREFIX}{col_name}"

        # if col_name is in source_info, use that value
        if col_name in source_info:
            # Use value from source_info dictionary
            source_value = source_info[col_name]
            source_values = pa.array([source_value] * num_rows, type=pa.large_string())
        # if col_name is in existing_source_info, use that column
        elif col_name in existing_source_info:
            # Use existing source_info column, but convert to large_string
            existing_col = existing_source_info[col_name]
            if existing_col.type == pa.large_string():
                source_values = existing_col
            else:
                # Convert to large_string
                source_values = pa.compute.cast(existing_col, pa.large_string())  # type: ignore

        else:
            # Use null values
            source_values = pa.array([None] * num_rows, type=pa.large_string())

        source_info_columns.append(source_values)
        source_info_column_names.append(source_info_col_name)

    # Step 3: Create the final table
    data_table: pa.Table = pa.Table.from_arrays(data_columns, names=data_column_names)
    source_info_table: pa.Table = pa.Table.from_arrays(
        source_info_columns, names=source_info_column_names
    )
    return data_table, source_info_table


class ArrowDatagram:
    """
    An immutable datagram implementation using a PyArrow Table backend.
    TODO: handle RecordBatch in addition to table

    This basic datagram provides functionality for type handling,
    semantic conversion, and dict-based content representation while maintaining
    immutability of the underlying data.


    Initialize ArrowDatagram with a PyArrow table.

    Args:
        data: Source data mapping
        typespec: Optional type specification for fields
        semantic_converter: Optional converter for semantic types
        semantic_type_registry: Registry for semantic type lookup
        arrow_hasher: Optional hasher for Arrow table content
    """

    def __init__(
        self,
        table: pa.Table,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
    ) -> None:
        # normalize the table to ensure it contains proper source columns
        if len(table) != 1:
            raise ValueError(
                "Table must contain exactly one row to be a valid datagram."
            )

        # TODO: add check for compatible types, especially of str being pa.large_string
        self._table = table

        # create semantic converter
        # TODO: consider some validation of passed semantic_converter
        if semantic_converter is None:
            if semantic_type_registry is None:
                raise ValueError(
                    "Semantic type registry must be provided if semantic converter is not specified."
                )
            semantic_converter = SemanticConverter.from_semantic_schema(
                schemas.SemanticSchema.from_arrow_schema(
                    self._table.schema,
                    semantic_type_registry,
                )
            )
        self._semantic_converter = semantic_converter
        self._arrow_hasher = arrow_hasher
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_python_dict: dict[str, DataValue] | None = None
        self._cached_content_hash: str | None = None

    def as_table(
        self,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        return self._table

    def as_dict(self) -> dict[str, DataValue]:
        """Return dictionary representation of the datagram."""
        if self._cached_python_dict is None:
            self._cached_python_dict = self._semantic_converter.from_arrow_to_python(
                self._table
            )[0]
        assert self._cached_python_dict is not None, "Cached dict should not be None"
        return dict(self._cached_python_dict)

    def content_hash(
        self,
    ) -> str:
        """
        Calculate and return content hash of the datagram.

        Returns:
            Hash string of the datagram content
        """
        if self._cached_content_hash is None:
            if self._arrow_hasher is None:
                raise ValueError(
                    "Arrow hasher must be provided to calculate content hash."
                )
            self._cached_content_hash = self._arrow_hasher.hash_table(
                self.as_table(),
                prefix_hasher_id=True,
            )
        return self._cached_content_hash

    def keys(self) -> tuple[str, ...]:
        return tuple(self._table.column_names)

    def types(self) -> schemas.PythonSchema:
        """Return copy of the Python schema."""
        if self._cached_python_schema is None:
            self._cached_python_schema = (
                self._semantic_converter.from_arrow_to_python_schema(self._table.schema)
            )
        return self._cached_python_schema.copy()

    @classmethod
    def _from_copy(
        cls,
        table: pa.Table,
        python_schema: schemas.PythonSchema,
        semantic_converter: SemanticConverter,
        hash_keys: tuple[str, ...],
        arrow_hasher: hp.ArrowHasher,
    ) -> Self:
        """Create a new instance from copy without full initialization."""
        instance = cls.__new__(cls)
        instance._table = table
        instance._semantic_converter = semantic_converter
        instance._arrow_hasher = arrow_hasher

        # Set attributes directly
        instance._cached_content_hash = None

        return instance

    def copy(self) -> Self:
        """Return a copy of the datagram."""
        new_datagram = self.__class__(
            self._table,
            semantic_converter=self._semantic_converter,
            arrow_hasher=self._arrow_hasher,
        )
        new_datagram._cached_python_schema = self._cached_python_schema
        new_datagram._cached_python_dict = self._cached_python_dict
        new_datagram._cached_python_dict = self._cached_python_dict
        return new_datagram

    def __repr__(self) -> str:
        """Return string representation."""
        return f"{self.as_dict()}"


class ArrowTag(ArrowDatagram):
    """
    A tag implementation using Arrow table backend.

    Represents a single-row Arrow table that can be converted to Python
    dictionary representation while caching computed values for efficiency.

    Initialize with an Arrow table.

        Args:
            table: Single-row Arrow table representing the tag

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        table: pa.Table,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowTag should only contain a single row, "
                "as it represents a single tag."
            )
        super().__init__(
            table=table,
            semantic_converter=semantic_converter,
            semantic_type_registry=semantic_type_registry,
            arrow_hasher=arrow_hasher,
        )


class ArrowPacket(ArrowDatagram):
    """
    Arrow table-based packet implementation with comprehensive features.

    A packet implementation that uses Arrow tables as the primary storage format,
    providing efficient memory usage and columnar data operations while supporting
    source information tracking and content hashing.


    Initialize ArrowPacket with Arrow table and configuration.

        Args:
            table: Single-row Arrow table representing the packet
            source_info: Optional source information mapping
            semantic_converter: Optional semantic converter
            semantic_type_registry: Registry for semantic types
            finger_print: Optional fingerprint for tracking
            arrow_hasher: Optional Arrow hasher
            post_hash_callback: Optional callback after hash calculation
            skip_source_info_extraction: Whether to skip source info processing

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        data: pa.Table,
        source_info: dict[str, str | None] | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
        skip_source_info_extraction: bool = False,
    ) -> None:
        if len(data) != 1:
            raise ValueError(
                "ArrowPacket should only contain a single row, "
                "as it represents a single packet."
            )
        if source_info is None:
            source_info = {}

        if not skip_source_info_extraction:
            # normalize the table to ensure it has the expected source_info columns
            data_table, self._source_info_table = prepare_data_and_source_tables(
                data, source_info
            )
        else:
            data_columns: tuple[str, ...] = tuple(
                [c for c in data.column_names if not c.startswith(SOURCE_INFO_PREFIX)]
            )
            source_columns = [f"{SOURCE_INFO_PREFIX}{c}" for c in data_columns]
            # Add conversion to large_string type
            data_table = data.select(data_columns)
            self._source_info_table = data.select(source_columns)

        super().__init__(
            data_table,
            semantic_converter=semantic_converter,
            semantic_type_registry=semantic_type_registry,
            arrow_hasher=arrow_hasher,
        )

        self._cached_source_info: dict[str, str | None] | None = None
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_content_hash: str | None = None

    def as_table(
        self,
        include_source: bool = False,
    ) -> pa.Table:
        table = super().as_table()
        if include_source:
            # add source_info only for existing data columns
            table = arrow_utils.hstack_tables(
                table,
                self._source_info_table.select(
                    [f"{SOURCE_INFO_PREFIX}{c}" for c in table.column_names]
                ),
            )
        return table

    def as_dict(self, include_source: bool = False) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        return_dict = super().as_dict()
        if include_source:
            return_dict.update(
                {f"{SOURCE_INFO_PREFIX}{k}": v for k, v in self.source_info().items()}
            )
        return return_dict

    def as_datagram(self, include_source: bool = False) -> ArrowDatagram:
        table = self.as_table(include_source=include_source)
        return ArrowDatagram(
            table,
            semantic_converter=self._semantic_converter,
            arrow_hasher=self._arrow_hasher,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        if self._cached_source_info is None:
            self._cached_source_info = {
                k.removeprefix(SOURCE_INFO_PREFIX): v
                for k, v in self._source_info_table.to_pylist()[0].items()
            }
        return self._cached_source_info.copy()

    def copy(self) -> Self:
        # TODO: restructure copy to allow for better inheritance and expansion
        new_packet = self.__class__(
            self.as_table(),
            self.source_info(),
            semantic_converter=self._semantic_converter,
            arrow_hasher=self._arrow_hasher,
            skip_source_info_extraction=True,
        )
        new_packet._cached_source_info = self._cached_source_info
        new_packet._cached_python_dict = self._cached_python_dict
        new_packet._cached_python_schema = self._cached_python_schema
        new_packet._cached_content_hash = self._cached_content_hash

        return new_packet


# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[dp.Tag, Collection[dp.Packet]]
"""Type alias for a batch: a tuple containing a tag and collection of packets."""
