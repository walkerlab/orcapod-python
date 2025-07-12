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

from orcapod.types.core import DataValue, StoreValue
from typing import TypeAlias, cast, Self
from collections.abc import Callable, Mapping, Collection
from orcapod.types import TypeSpec, default_registry
from orcapod.protocols import data_protocols as dp, hashing_protocols as hp
from orcapod.types.semantic_type_registry import SemanticTypeRegistry
from orcapod.types.core import TypeHandler
from orcapod.types import schemas
from orcapod.types.typespec_utils import get_typespec_from_dict
import pyarrow as pa
import logging

from orcapod.hashing.defaults import get_default_arrow_hasher

# Constants used for source info keys
SOURCE_INFO_PREFIX = "_source_info_"


# TODO: move this to a separate module
def hstack_tables(*tables: pa.Table) -> pa.Table:
    """
    Horizontally stack multiple PyArrow tables by concatenating their columns.

    All input tables must have the same number of rows and unique column names.

    Args:
        *tables: Variable number of PyArrow tables to stack horizontally

    Returns:
        Combined PyArrow table with all columns from input tables

    Raises:
        ValueError: If no tables provided, tables have different row counts,
                   or duplicate column names are found
    """
    if len(tables) == 0:
        raise ValueError("At least one table is required for horizontal stacking.")
    if len(tables) == 1:
        return tables[0]

    N = len(tables[0])
    for table in tables[1:]:
        if len(table) != N:
            raise ValueError(
                "All tables must have the same number of rows for horizontal stacking."
            )

    # create combined column names
    all_column_names = []
    all_columns = []
    all_names = set()
    for i, table in enumerate(tables):
        if overlap := set(table.column_names).intersection(all_names):
            raise ValueError(
                f"Duplicate column names {overlap} found when stacking table at index {i}: {table}"
            )
        all_names.update(table.column_names)
        all_column_names += table.column_names
        all_columns += table.columns

    return pa.Table.from_arrays(all_columns, names=all_column_names)


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

SemanticStore: TypeAlias = Mapping[str, StoreValue]
PythonStore: TypeAlias = Mapping[str, DataValue]


def check_arrow_schema_compatibility(
    incoming_schema: pa.Schema, current_schema: pa.Schema
) -> tuple[bool, list[str]]:
    """
    Check if incoming schema is compatible with current schema.

    Args:
        incoming_schema: Schema to validate
        current_schema: Expected schema to match against

    Returns:
        Tuple of (is_compatible, list_of_errors)
    """
    errors = []

    # Create lookup dictionaries for efficient access
    incoming_fields = {field.name: field for field in incoming_schema}
    current_fields = {field.name: field for field in current_schema}

    # Check each field in current_schema
    for field_name, current_field in current_fields.items():
        if field_name not in incoming_fields:
            errors.append(f"Missing field '{field_name}' in incoming schema")
            continue

        incoming_field = incoming_fields[field_name]

        # Check data type compatibility
        if not current_field.type.equals(incoming_field.type):
            errors.append(
                f"Type mismatch for field '{field_name}': "
                f"expected {current_field.type}, got {incoming_field.type}"
            )

        # Check semantic_type metadata if present in current schema
        current_metadata = current_field.metadata or {}
        incoming_metadata = incoming_field.metadata or {}

        if b"semantic_type" in current_metadata:
            expected_semantic_type = current_metadata[b"semantic_type"]

            if b"semantic_type" not in incoming_metadata:
                errors.append(
                    f"Missing 'semantic_type' metadata for field '{field_name}'"
                )
            elif incoming_metadata[b"semantic_type"] != expected_semantic_type:
                errors.append(
                    f"Semantic type mismatch for field '{field_name}': "
                    f"expected {expected_semantic_type.decode()}, "
                    f"got {incoming_metadata[b'semantic_type'].decode()}"
                )
        elif b"semantic_type" in incoming_metadata:
            errors.append(
                f"Unexpected 'semantic_type' metadata for field '{field_name}': "
                f"{incoming_metadata[b'semantic_type'].decode()}"
            )

    return len(errors) == 0, errors


class SemanticConverter:
    """
    Converts data between different representations (Python, semantic stores, Arrow tables).

    This class handles the conversion between Python data structures, semantic stores
    (which use storage-optimized types), and Arrow tables while maintaining type
    information and semantic type metadata.
    """

    @staticmethod
    def prepare_handler(
        semantic_schema: schemas.SemanticSchema,
        semantic_type_registry: SemanticTypeRegistry,
    ) -> dict[str, TypeHandler]:
        """
        Prepare type handlers for semantic type conversion.

        Args:
            semantic_schema: Schema containing semantic type information
            semantic_type_registry: Registry for looking up type handlers

        Returns:
            Dictionary mapping field names to their type handlers
        """
        handler_lut = {}
        for key, (_, semantic_type) in semantic_schema.items():
            if semantic_type is None:
                continue  # Skip keys without semantic type
            handler_lut[key] = semantic_type_registry.get_handler_by_semantic_type(
                semantic_type
            )
        return handler_lut

    @classmethod
    def from_typespec(
        cls, typespec: TypeSpec, semantic_type_registry: SemanticTypeRegistry
    ) -> "SemanticConverter":
        """
        Create a SemanticConverter from a basic Python type specification dictionary (TypeSpec).

        Args:
            typespec: Type specification dictionary
            semantic_type_registry: Registry for semantic type lookup

        Returns:
            New SemanticConverter instance
        """
        semantic_schema = schemas.from_typespec_to_semantic_schema(
            typespec, semantic_type_registry
        )
        python_schema = schemas.PythonSchema(typespec)
        handler_lut = cls.prepare_handler(semantic_schema, semantic_type_registry)
        return cls(python_schema, semantic_schema, handler_lut)

    @classmethod
    def from_arrow_schema(
        cls, arrow_schema: pa.Schema, semantic_type_registry: SemanticTypeRegistry
    ) -> "SemanticConverter":
        """
        Create a SemanticConverter from an Arrow schema.

        Args:
            arrow_schema: PyArrow schema with semantic type metadata
            semantic_type_registry: Registry for semantic type lookup

        Returns:
            New SemanticConverter instance
        """
        semantic_schema = schemas.from_arrow_schema_to_semantic_schema(arrow_schema)
        python_schema = schemas.from_semantic_schema_to_python_schema(
            semantic_schema, semantic_type_registry=semantic_type_registry
        )
        handler_lut = cls.prepare_handler(semantic_schema, semantic_type_registry)
        return cls(python_schema, semantic_schema, handler_lut)

    def __init__(
        self,
        python_schema: schemas.PythonSchema,
        semantic_schema: schemas.SemanticSchema,
        handler_lut: dict[str, TypeHandler] | None = None,
    ):
        """
        Initialize SemanticConverter with schemas and type handlers. This is not meant to be called directly.
        Use class methods like `from_arrow_schema` or `from_typespec` instead.

        Args:
            python_schema: Schema for Python data types
            semantic_schema: Schema for semantic types
            handler_lut: Optional dictionary of type handlers for conversion
        """
        self.python_schema = python_schema
        self.semantic_schema = semantic_schema
        self.arrow_schema = schemas.from_semantic_schema_to_arrow_schema(
            semantic_schema, include_source_info=False
        )
        if handler_lut is None:
            handler_lut = {}
        self.handler_lut = handler_lut

    def from_semantic_store_to_python_store(
        self, semantic_store: SemanticStore
    ) -> PythonStore:
        """
        Convert a semantic store to a Python store.

        Args:
            semantic_store: Store (dict) with data stored in semantic (storage-optimized) types

        Returns:
            Store with Python native types
        """
        python_store = dict(semantic_store)
        for key, handler in self.handler_lut.items():
            python_store[key] = handler.storage_to_python(semantic_store[key])
        return python_store

    def from_python_store_to_semantic_store(
        self, python_store: PythonStore
    ) -> SemanticStore:
        """
        Convert a Python store to a semantic store.

        Args:
            python_store: Store with Python native types

        Returns:
            Store with semantic (storage-optimized) types
        """
        semantic_store = dict(python_store)
        for key, handler in self.handler_lut.items():
            semantic_store[key] = handler.python_to_storage(python_store[key])
        return semantic_store  # type: ignore[return-value]

    def from_semantic_store_to_arrow_table(
        self, semantic_store: SemanticStore
    ) -> pa.Table:
        """Convert a semantic store to an Arrow table."""
        return pa.Table.from_pylist([semantic_store], schema=self.arrow_schema)

    def from_python_store_to_arrow_table(self, python_store: PythonStore) -> pa.Table:
        """Convert a Python store to an Arrow table."""
        semantic_store = self.from_python_store_to_semantic_store(python_store)
        return self.from_semantic_store_to_arrow_table(semantic_store)

    def from_arrow_table_to_semantic_stores(
        self, arrow_table: pa.Table
    ) -> list[SemanticStore]:
        """Convert an Arrow table to a list of semantic stores."""
        self.verify_compatible_arrow_schema(arrow_table.schema)
        return arrow_table.to_pylist()  # Ensure the table is materialized

    def from_arrow_table_to_python_stores(
        self, arrow_table: pa.Table
    ) -> list[PythonStore]:
        """Convert an Arrow table to a list of Python stores."""
        return [
            self.from_semantic_store_to_python_store(semantic_store)
            for semantic_store in self.from_arrow_table_to_semantic_stores(arrow_table)
        ]

    def verify_compatible_arrow_schema(self, arrow_schema: pa.Schema):
        """
        Verify that an Arrow schema is compatible with the expected schema.

        Args:
            arrow_schema: Schema to verify

        Raises:
            ValueError: If schemas are incompatible
        """
        compatible, errors = check_arrow_schema_compatibility(
            arrow_schema, self.arrow_schema
        )
        if not compatible:
            raise ValueError(
                "Arrow table schema is not compatible with the expected schema: "
                + ", ".join(errors)
            )


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
        inferred_typespec = get_typespec_from_dict(self)
        for key in self:
            if key not in verified_typespec:
                verified_typespec[key] = inferred_typespec[key]
        self._python_schema = schemas.PythonSchema(verified_typespec)

        # create semantic converter
        if semantic_converter is not None:
            if semantic_converter.python_schema != self._python_schema:
                raise ValueError(
                    "Incompatible Python schema between packet and semantic converter: "
                    + str(self._python_schema)
                    + " vs "
                    + str(semantic_converter.python_schema)
                )
        else:
            semantic_converter = SemanticConverter.from_typespec(
                self._python_schema,
                semantic_type_registry or default_registry,
            )
        self.semantic_converter = semantic_converter

        if arrow_hasher is None:
            arrow_hasher = get_default_arrow_hasher()
        self.arrow_hasher = arrow_hasher

        self._cached_table: pa.Table | None = None
        self._cached_content_hash: str | None = None

    def as_table(
        self,
        keep_columns: Collection[str] | None = None,
        drop_columns: Collection[str] | None = None,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        if keep_columns is not None and drop_columns is not None:
            logger.warning(
                "It is not recommended to provide both keep_columns and drop_columns. The resulting behavior may not be as expected."
            )
        if self._cached_table is None:
            self._cached_table = (
                self.semantic_converter.from_python_store_to_arrow_table(self.as_dict())
            )
            assert self._cached_table is not None, "Cached table should not be None"
        processed_table = self._cached_table
        if keep_columns is not None:
            processed_table = processed_table.select(list(keep_columns))

        if drop_columns is not None:
            processed_table = processed_table.drop(list(drop_columns))

        return processed_table

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
            self._cached_content_hash = self.arrow_hasher.hash_table(
                self.as_table(),
                prefix_hasher_id=True,
            )
        return self._cached_content_hash

    # use keys() implementation from dict

    def types(self) -> schemas.PythonSchema:
        """Return copy of the Python schema."""
        return self._python_schema.copy()

    def copy(self) -> Self:
        """Return a copy of the datagram."""
        return self.__class__(
            self,
            typespec=self.types(),
            semantic_converter=self.semantic_converter,
            arrow_hasher=self.arrow_hasher,
        )


class PythonDictTag(dict[str, DataValue]):
    """
    A simple tag implementation using Python dictionary.

    Represents a tag (metadata) as a dictionary that can be converted
    to different representations like Arrow tables.
    """

    def as_dict(self) -> dict[str, DataValue]:
        """Return dictionary representation."""
        return dict(self)

    def as_table(self) -> pa.Table:
        """Convert to Arrow table representation."""
        return pa.Table.from_pylist([self])

    def types(self) -> schemas.PythonSchema:
        """
        Return Python schema (basic implementation).

        Note: This is a simplified implementation that assumes all values are strings.
        """
        # TODO: provide correct implementation
        return schemas.PythonSchema({k: str for k in self.keys()})


class ArrowTag:
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

    def __init__(self, table: pa.Table) -> None:
        self.table = table
        if len(table) != 1:
            raise ValueError(
                "ArrowTag should only contain a single row, "
                "as it represents a single tag."
            )
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_python_dict: dict[str, DataValue] | None = None

    def keys(self) -> tuple[str, ...]:
        """Return column names as a tuple."""
        return tuple(self.table.column_names)

    def types(self) -> schemas.PythonSchema:
        """
        Return Python schema derived from Arrow schema.

        Returns:
            TypeSpec information returned as PythonSchema.
        """
        if self._cached_python_schema is None:
            self._cached_python_schema = schemas.from_arrow_schema_to_semantic_schema(
                self.table.schema
            ).storage_schema
        return self._cached_python_schema.copy()

    def as_dict(self) -> dict[str, DataValue]:
        """
        Convert to Python dictionary representation.

        Returns:
            Dictionary with tag data
        """
        if self._cached_python_dict is None:
            self._cached_python_dict = cast(
                dict[str, DataValue], self.table.to_pylist()[0]
            )
        return self._cached_python_dict

    def as_table(self) -> pa.Table:
        """Return the underlying Arrow table."""
        return self.table

    def clear_cache(self) -> None:
        """Clear cached Python representations."""
        self._cached_python_schema = None
        self._cached_python_dict = None

    def __repr__(self) -> str:
        """Return string representation."""
        return f"{self.as_dict()}"


class PythonDictPacket2(DictDatagram):
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
        keep_columns: Collection[str] | None = None,
        drop_columns: Collection[str] | None = None,
        include_source: bool = False,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        table = super().as_table(keep_columns=keep_columns, drop_columns=drop_columns)
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
            table = hstack_tables(table, source_info_table)
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

    def content_hash(self) -> str:
        """
        Calculate content hash excluding source information.

        Returns:
            Hash string of the packet content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self.arrow_hasher.hash_table(
                self.as_table(include_source=False), prefix_hasher_id=True
            )
        return self._cached_content_hash

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

    def copy(self) -> "PythonDictPacket2":
        """Return a shallow copy of the packet."""
        new_packet = PythonDictPacket2(self, self.source_info())
        new_packet._cached_table = self._cached_table
        new_packet._cached_content_hash = self._cached_content_hash
        new_packet._python_schema = self._python_schema.copy()
        new_packet.semantic_converter = self.semantic_converter
        new_packet.arrow_hasher = self.arrow_hasher
        return new_packet


class PythonDictPacket(dict[str, DataValue]):
    """
    Dictionary-based Packet with source tracking and hashing.

    A dictionary-based packet that maintains source information, supports
    type specifications, and provides content hashing with optional callbacks.
    Includes comprehensive conversion capabilities to Arrow tables.

    Initialize packet with comprehensive configuration options.

    Args:
        data: Primary packet data
        source_info: Optional source information mapping
        typespec: Optional type specification
        finger_print: Optional fingerprint for tracking
        semantic_converter: Optional semantic converter
        semantic_type_registry: Registry for semantic types
        arrow_hasher: Optional Arrow hasher
        post_hash_callback: Optional callback after hash calculation

    """

    @classmethod
    def create_from(
        cls,
        object: dp.Packet,
        finger_print: str | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
        post_hash_callback: Callable[[str, str], None] | None = None,
    ) -> "PythonDictPacket":
        """
        Create a PythonDictPacket from another packet object.

        Args:
            object: Source packet to copy from
            finger_print: Optional fingerprint identifier
            semantic_converter: Optional semantic converter
            semantic_type_registry: Registry for semantic types
            arrow_hasher: Optional Arrow hasher
            post_hash_callback: Optional callback after hash calculation

        Returns:
            New PythonDictPacket instance
        """
        if isinstance(object, PythonDictPacket):
            return object.copy()

        new_packet = PythonDictPacket(
            object.as_dict(include_source=False),
            object.source_info(),
            dict(object.types()),
            finger_print=finger_print,
            semantic_converter=semantic_converter,
            semantic_type_registry=semantic_type_registry,
            arrow_hasher=arrow_hasher,
            post_hash_callback=post_hash_callback,
        )
        return new_packet

    def __init__(
        self,
        data: dict[str, DataValue],
        source_info: dict[str, str | None] | None = None,
        typespec: TypeSpec | None = None,
        finger_print: str | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
        post_hash_callback: Callable[[str, str], None] | None = None,
    ) -> None:
        # normalize the data content and remove any source info keys
        data = {k: v for k, v in data.items() if not k.startswith(SOURCE_INFO_PREFIX)}
        contained_source_info = {
            k.removeprefix(SOURCE_INFO_PREFIX): v
            for k, v in data.items()
            if k.startswith(SOURCE_INFO_PREFIX)
        }
        super().__init__(data)

        self._source_info = {**contained_source_info, **(source_info or {})}

        verified_typespec = {}
        if typespec is not None:
            verified_typespec = dict(typespec)
        inferred_typespec = get_typespec_from_dict(self)
        for key in self:
            if key not in verified_typespec:
                verified_typespec[key] = inferred_typespec[key]
        self._typespec = verified_typespec

        self._python_schema = schemas.PythonSchema(self._typespec)

        if semantic_converter is not None:
            if semantic_converter.python_schema != self._python_schema.with_source_info:
                raise ValueError(
                    "Incompatible Python schema between packet and semantic converter: "
                    + str(self._python_schema.with_source_info)
                    + " vs "
                    + str(semantic_converter.python_schema)
                )
        else:
            semantic_converter = SemanticConverter.from_typespec(
                self._python_schema.with_source_info,
                semantic_type_registry or default_registry,
            )
        self.semantic_converter = semantic_converter

        self._finger_print = finger_print
        self._post_hash_callback = post_hash_callback
        self._cached_table: pa.Table | None = None
        self._cached_content_hash: str | None = None

        if arrow_hasher is None:
            arrow_hasher = get_default_arrow_hasher()
        self.arrow_hasher = arrow_hasher

    def as_table(self, include_source: bool = False) -> pa.Table:
        """Convert the packet to an Arrow table."""
        if self._cached_table is None:
            self._cached_table = (
                self.semantic_converter.from_python_store_to_arrow_table(
                    self.as_dict(include_source=True)
                )
            )
            assert self._cached_table is not None, "Cached table should not be None"
        if include_source:
            return self._cached_table
        else:
            # drop source info columns if not needed
            return self._cached_table.select(list(self.keys()))

    def as_dict(self, include_source: bool = False) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = self.copy()
        if include_source:
            for key, value in self.source_info().items():
                dict_copy[f"{SOURCE_INFO_PREFIX}{key}"] = value
        return dict_copy

    def content_hash(self) -> str:
        """
        Calculate and return content hash.

        Computes hash of packet data content (thus excluding source info) and
        optionally triggers post-hash callback if configured.

        Returns:
            Hash string of the packet content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self.arrow_hasher.hash_table(
                self.as_table(include_source=False), prefix_hasher_id=True
            )
            if self._post_hash_callback is not None and self._finger_print is not None:
                self._post_hash_callback(self._finger_print, self._cached_content_hash)
        return self._cached_content_hash

    # use keys() implementation from dict

    def types(self) -> schemas.PythonSchema:
        """Return packet data type information as PythonSchema (dict mapping field names to types)."""
        return self._python_schema.copy()

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Dictionary mapping field names to their source info
        """
        return {key: self._source_info.get(key, None) for key in self.keys()}

    def copy(self) -> "PythonDictPacket":
        """Return a shallow copy of the packet."""
        new_packet = PythonDictPacket(self, self.source_info())
        new_packet._finger_print = self._finger_print
        new_packet._cached_table = self._cached_table
        new_packet._cached_content_hash = self._cached_content_hash
        new_packet._python_schema = self._python_schema.copy()
        new_packet.semantic_converter = self.semantic_converter
        new_packet.arrow_hasher = self.arrow_hasher
        new_packet._post_hash_callback = self._post_hash_callback
        return new_packet


def process_table_with_source_info(
    table: pa.Table, source_info: dict[str, str | None] | None = None
) -> tuple[tuple[str, ...], pa.Table]:
    """
    Process a table to ensure proper source_info columns.

    Args:
        table: Input PyArrow table
        source_info: optional dictionary mapping column names to source info values. If present,
                     it will take precedence over existing source_info columns in the table.

    Returns:
        Processed table with source_info columns
    """
    if source_info is None:
        source_info = {}

    # Step 1: Separate source_info columns from regular columns
    regular_columns = []
    regular_names = []
    existing_source_info = {}

    for i, name in enumerate(table.column_names):
        if name.startswith(SOURCE_INFO_PREFIX):
            # Extract the base column name
            base_name = name.removeprefix(SOURCE_INFO_PREFIX)
            existing_source_info[base_name] = table.column(i)
        else:
            regular_columns.append(table.column(i))
            regular_names.append(name)

    # Step 2: Create source_info columns for each regular column
    final_columns = []
    final_names = []

    # Add all regular columns first
    final_columns.extend(regular_columns)
    final_names.extend(regular_names)

    # Create source_info columns for each regular column
    num_rows = table.num_rows

    for col_name in regular_names:
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

        final_columns.append(source_values)
        final_names.append(source_info_col_name)

    # Step 3: Create the final table
    result: pa.Table = pa.Table.from_arrays(final_columns, names=final_names)
    return tuple(regular_names), result


class ArrowPacket:
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

    @classmethod
    def create_from(
        cls,
        object: dp.Packet,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        finger_print: str | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
        post_hash_callback: Callable[[str, str], None] | None = None,
    ) -> "ArrowPacket":
        """
        Create an ArrowPacket from another packet object.

        Args:
            object: Source packet to copy from
            semantic_converter: Optional semantic converter
            semantic_type_registry: Registry for semantic types
            finger_print: Optional fingerprint identifier
            arrow_hasher: Optional Arrow hasher
            post_hash_callback: Optional callback after hash calculation

        Returns:
            New ArrowPacket instance
        """
        if isinstance(object, ArrowPacket):
            return object.copy()

        new_packet = ArrowPacket(
            object.as_table(include_source=True),
            semantic_converter=semantic_converter,
            semantic_type_registry=semantic_type_registry,
            finger_print=finger_print,
            arrow_hasher=arrow_hasher,
            post_hash_callback=post_hash_callback,
            skip_source_info_extraction=True,
        )
        return new_packet

    def __init__(
        self,
        table: pa.Table,
        source_info: dict[str, str | None] | None = None,
        semantic_converter: SemanticConverter | None = None,
        semantic_type_registry: SemanticTypeRegistry | None = None,
        finger_print: str | None = None,
        arrow_hasher: hp.ArrowHasher | None = None,
        post_hash_callback: Callable[[str, str], None] | None = None,
        skip_source_info_extraction: bool = False,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowPacket should only contain a single row, "
                "as it represents a single packet."
            )
        if source_info is None:
            source_info = {}

        if not skip_source_info_extraction:
            # normalize the table to ensure it has the expected source_info columns
            self._keys, self._arrow_table = process_table_with_source_info(
                table, source_info
            )
        else:
            self._keys: tuple[str, ...] = tuple(
                [c for c in table.column_names if not c.startswith(SOURCE_INFO_PREFIX)]
            )
            for k in self._keys:
                if f"{SOURCE_INFO_PREFIX}{k}" not in table.column_names:
                    raise ValueError(
                        f"Source info column '{SOURCE_INFO_PREFIX}{k}' is missing in the table."
                    )
            self._arrow_table = table

        self._finger_print = finger_print
        self._post_hash_callback = post_hash_callback

        if semantic_converter is not None:
            check_arrow_schema_compatibility(
                semantic_converter.arrow_schema, self._arrow_table.schema
            )
        else:
            semantic_converter = SemanticConverter.from_arrow_schema(
                self._arrow_table.schema, semantic_type_registry or default_registry
            )
        self.semantic_converter = semantic_converter

        if arrow_hasher is None:
            arrow_hasher = get_default_arrow_hasher()
        self.arrow_hasher = arrow_hasher

        self._cached_python_packet: PythonStore | None = None
        self._cached_content_hash: str | None = None
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_source_info: dict[str, str | None] | None = None

    def as_table(self, include_source: bool = False) -> pa.Table:
        """Return the Arrow table representation of the packet."""
        base_table = self._arrow_table
        if not include_source:
            # Select only the keys that are not source info
            base_table = base_table.select(self._keys)
        return base_table

    def as_dict(self, include_source: bool = False) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        if self._cached_python_packet is None:
            self._cached_python_packet = (
                self.semantic_converter.from_arrow_table_to_python_stores(
                    self._arrow_table
                )[0]
            )
        if include_source:
            return dict(self._cached_python_packet)

        return {k: self._cached_python_packet[k] for k in self._keys}

    def content_hash(self) -> str:
        """
        Calculate and return content hash.

        Computes hash of the Arrow table content and optionally
        triggers post-hash callback if configured.

        Returns:
            Hash string of the packet content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self.arrow_hasher.hash_table(
                self._arrow_table, prefix_hasher_id=True
            )
            if self._post_hash_callback is not None and self._finger_print is not None:
                self._post_hash_callback(self._finger_print, self._cached_content_hash)
        return self._cached_content_hash

    def types(self) -> schemas.PythonSchema:
        """Return packet data type information as PythonSchema (dict mapping field names to types)."""
        return self.semantic_converter.python_schema.copy()

    def keys(self) -> tuple[str, ...]:
        """Return the keys of the packet."""
        return tuple(self._keys)

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        if self._cached_source_info is None:
            self._cached_source_info = {
                k: self._arrow_table[f"{SOURCE_INFO_PREFIX}{k}"][0].as_py()
                for k in self._keys
            }
        return self._cached_source_info.copy()

    def copy(self) -> "ArrowPacket":
        """Return a shallow copy of the packet."""
        new_packet = ArrowPacket(
            self._arrow_table,
            semantic_converter=self.semantic_converter,
            finger_print=self._finger_print,
            arrow_hasher=self.arrow_hasher,
            post_hash_callback=self._post_hash_callback,
            skip_source_info_extraction=True,
        )
        new_packet._cached_content_hash = self._cached_content_hash
        new_packet._cached_source_info = (
            self._cached_source_info.copy()
            if self._cached_source_info is not None
            else None
        )
        new_packet._cached_python_packet = (
            dict(self._cached_python_packet)
            if self._cached_python_packet is not None
            else None
        )
        return new_packet

    def __repr__(self) -> str:
        """Return string representation."""
        return f"{self.as_dict(include_source=False)}"


# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[dp.Tag, Collection[dp.Packet]]
"""Type alias for a batch: a tuple containing a tag and collection of packets."""
