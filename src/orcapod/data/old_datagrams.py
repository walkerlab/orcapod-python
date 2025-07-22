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

from hmac import new
import logging
from abc import ABC, abstractmethod
from collections.abc import Collection, Iterator, Mapping
from types import new_class
from typing import Self, TypeAlias, cast

from matplotlib.pyplot import arrow
import pyarrow as pa

from orcapod.data.system_constants import orcapod_constants as constants
from orcapod.data.context import (
    DataContext,
)
from orcapod.protocols import data_protocols as dp
from orcapod.protocols import hashing_protocols as hp
from orcapod.types import TypeSpec, schemas, typespec_utils
from orcapod.types import typespec_utils as tsutils
from orcapod.types.core import DataValue
from orcapod.types.semantic_converter import SemanticConverter
from orcapod.utils import arrow_utils

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

    def __or__(self, other: Mapping[str, DataValue]) -> Self:
        """
        Create a new ImmutableDict by merging with another mapping.

        Args:
            other: Another mapping to merge with

        Returns:
            A new ImmutableDict containing the combined data
        """
        return self.__class__(self._data | dict(other))


def contains_prefix_from(column: str, prefixes: Collection[str]) -> bool:
    """
    Check if a column name matches any of the given prefixes.

    Args:
        column: Column name to check
        prefixes: Collection of prefixes to match against

    Returns:
        True if the column starts with any of the prefixes, False otherwise
    """
    for prefix in prefixes:
        if column.startswith(prefix):
            return True
    return False


class BaseDatagram(ABC):
    """
    Abstract base class for immutable datagram implementations.

    Provides shared functionality and enforces consistent interface across
    different storage backends (dict, Arrow table, etc.). Concrete subclasses
    must implement the abstract methods to handle their specific storage format.

    The base class only manages the data context key string - how that key
    is interpreted and used is left to concrete implementations.
    """

    def __init__(self, data_context: DataContext | str | None = None) -> None:
        """
        Initialize base datagram with data context.

        Args:
            data_context: Context for semantic interpretation. Can be a string key
                or a DataContext object, or None for default.
        """
        self._data_context = DataContext.resolve_data_context(data_context)

    # 1. Core Properties (Identity & Structure)
    @property
    def data_context_key(self) -> str:
        """Return the data context key."""
        return self._data_context.context_key

    @property
    @abstractmethod
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names."""
        ...

    # 2. Dict-like Interface (Data Access)
    @abstractmethod
    def __getitem__(self, key: str) -> DataValue:
        """Get data column value by key."""
        ...

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """Check if data column exists."""
        ...

    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        """Iterate over data column names."""
        ...

    @abstractmethod
    def get(self, key: str, default: DataValue = None) -> DataValue:
        """Get data column value with default."""
        ...

    # 3. Structural Information
    @abstractmethod
    def keys(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> tuple[str, ...]:
        """Return tuple of column names."""
        ...

    @abstractmethod
    def types(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> TypeSpec:
        """Return type specification for the datagram."""
        ...

    @abstractmethod
    def arrow_schema(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Schema:
        """Return the PyArrow schema for this datagram."""
        ...

    @abstractmethod
    def content_hash(self) -> str:
        """Calculate and return content hash of the datagram."""
        ...

    # 4. Format Conversions (Export)
    @abstractmethod
    def as_dict(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> dict[str, DataValue]:
        """Return dictionary representation of the datagram."""
        ...

    @abstractmethod
    def as_table(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Table:
        """Convert the datagram to an Arrow table."""
        ...

    # 5. Meta Column Operations
    @abstractmethod
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """Get a meta column value."""
        ...

    @abstractmethod
    def with_meta_columns(self, **updates: DataValue) -> Self:
        """Create new datagram with updated meta columns."""
        ...

    @abstractmethod
    def drop_meta_columns(self, *keys: str) -> Self:
        """Create new datagram with specified meta columns removed."""
        ...

    # 6. Data Column Operations
    @abstractmethod
    def select(self, *column_names: str) -> Self:
        """Create new datagram with only specified data columns."""
        ...

    @abstractmethod
    def drop(self, *column_names: str) -> Self:
        """Create new datagram with specified data columns removed."""
        ...

    @abstractmethod
    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        """Create new datagram with data columns renamed."""
        ...

    @abstractmethod
    def update(self, **updates: DataValue) -> Self:
        """Create new datagram with existing column values updated."""
        ...

    @abstractmethod
    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """Create new datagram with additional data columns."""
        ...

    # 7. Context Operations
    @abstractmethod
    def with_context_key(self, new_context_key: str) -> Self:
        """Create new datagram with different data context."""
        ...

    # 8. Utility Operations
    @abstractmethod
    def copy(self) -> Self:
        """Create a shallow copy of the datagram."""
        ...


class DictDatagram(BaseDatagram):
    """
    Immutable datagram implementation using dictionary as storage backend.

    This implementation uses composition (not inheritance from Mapping) to maintain
    control over the interface while leveraging dictionary efficiency for data access.
    Provides clean separation between data, meta, and context components.

    The underlying data is split into separate components:
    - Data dict: Primary business data columns
    - Meta dict: Internal system metadata with {orcapod.META_PREFIX} ('__') prefixes
    - Context: Data context information with {orcapod.CONTEXT_KEY}

    Future Packet subclass will also handle:
    - Source info: Data provenance with {orcapod.SOURCE_PREFIX} ('_source_') prefixes

    When exposing to external tools, semantic types are encoded as
    `_{semantic_type}_` prefixes (_path_config_file, _id_user_name).

    All operations return new instances, preserving immutability.

    Example:
        >>> data = {{
        ...     "user_id": 123,
        ...     "name": "Alice",
        ...     "__pipeline_version": "v2.1.0",
        ...     "{orcapod.CONTEXT_KEY}": "financial_v1"
        ... }}
        >>> datagram = DictDatagram(data)
        >>> updated = datagram.update(name="Alice Smith")
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        typespec: TypeSpec | None = None,
        semantic_converter: SemanticConverter | None = None,
        data_context: str | DataContext | None = None,
    ) -> None:
        """
        Initialize DictDatagram from dictionary data.

        Args:
            data: Source data mapping containing all column data.
            typespec: Optional type specification for fields.
            semantic_converter: Optional converter for semantic type handling.
                If None, will be created based on data context and inferred types.
            data_context: Data context for semantic type resolution.
                If None and data contains context column, will extract from data.

        Note:
            The input data is automatically split into data, meta, and context
            components based on column naming conventions.
        """
        # Parse through data and extract different column types
        data_columns = {}
        meta_columns = {}
        extracted_context = None

        for k, v in data.items():
            if k == constants.CONTEXT_KEY:
                # Extract data context but keep it separate from meta data
                if data_context is None:
                    extracted_context = v
                # Don't store context in meta_data - it's managed separately
            elif k.startswith(constants.META_PREFIX):
                # Double underscore = meta metadata
                meta_columns[k] = v
            else:
                # Everything else = user data (including _source_ and semantic types)
                data_columns[k] = v

        # Initialize base class with data context
        final_context = data_context or cast(str, extracted_context)
        super().__init__(final_context)

        # Store data and meta components separately (immutable)
        self._data = dict(data_columns)
        self._meta_data = dict(meta_columns)

        # Combine provided typespec info with inferred typespec from content
        # If the column value is None and no type spec is provided, defaults to str.
        self._data_python_schema = schemas.PythonSchema(
            tsutils.get_typespec_from_dict(
                self._data,
                typespec,
            )
        )

        # Create semantic converter
        if semantic_converter is None:
            semantic_converter = SemanticConverter.from_semantic_schema(
                self._data_python_schema.to_semantic_schema(
                    semantic_type_registry=self._data_context.semantic_type_registry
                ),
            )
        self.semantic_converter = semantic_converter

        # Create schema for meta data
        self._meta_python_schema = schemas.PythonSchema(
            tsutils.get_typespec_from_dict(
                self._meta_data,
                typespec=typespec,
            )
        )

        # Initialize caches
        self._cached_data_table: pa.Table | None = None
        self._cached_meta_table: pa.Table | None = None
        self._cached_content_hash: str | None = None
        self._cached_data_arrow_schema: pa.Schema | None = None
        self._cached_meta_arrow_schema: pa.Schema | None = None

    # 1. Core Properties (Identity & Structure)
    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names."""
        return tuple(self._meta_data.keys())

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """Get data column value by key."""
        if key not in self._data:
            raise KeyError(f"Data column '{key}' not found")
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        """Check if data column exists."""
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        """Iterate over data column names."""
        return iter(self._data)

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """Get data column value with default."""
        return self._data.get(key, default)

    # 3. Structural Information
    def keys(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> tuple[str, ...]:
        """Return tuple of column names."""
        # Start with data columns
        result_keys = list(self._data.keys())

        # Add context if requested
        if include_context:
            result_keys.append(constants.CONTEXT_KEY)

        # Add meta columns if requested
        if include_meta_columns:
            if include_meta_columns is True:
                result_keys.extend(self.meta_columns)
            elif isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                filtered_meta_cols = [
                    col
                    for col in self.meta_columns
                    if any(col.startswith(prefix) for prefix in include_meta_columns)
                ]
                result_keys.extend(filtered_meta_cols)

        return tuple(result_keys)

    def types(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> schemas.PythonSchema:
        """
        Return Python schema for the datagram.

        Args:
            include_meta_columns: Whether to include meta column types.
                - True: include all meta column types
                - Collection[str]: include meta column types matching these prefixes
                - False: exclude meta column types
            include_context: Whether to include context type

        Returns:
            Python schema
        """
        # Start with data schema
        schema = dict(self._data_python_schema)

        # Add context if requested
        if include_context:
            schema[constants.CONTEXT_KEY] = str

        # Add meta schema if requested
        if include_meta_columns and self._meta_data:
            if include_meta_columns is True:
                schema.update(self._meta_python_schema)
            elif isinstance(include_meta_columns, Collection):
                filtered_meta_schema = {
                    k: v
                    for k, v in self._meta_python_schema.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
                schema.update(filtered_meta_schema)

        return schemas.PythonSchema(schema)

    def arrow_schema(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Schema:
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_meta_columns: Whether to include meta columns in the schema.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context column in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        # Build data schema (cached)
        if self._cached_data_arrow_schema is None:
            self._cached_data_arrow_schema = (
                self.semantic_converter.from_python_to_arrow_schema(
                    self._data_python_schema
                )
            )

        all_schemas = [self._cached_data_arrow_schema]

        # Add context schema if requested
        if include_context:
            context_schema = pa.schema([pa.field(constants.CONTEXT_KEY, pa.string())])
            all_schemas.append(context_schema)

        # Add meta schema if requested
        if include_meta_columns and self._meta_data:
            if self._cached_meta_arrow_schema is None:
                self._cached_meta_arrow_schema = (
                    self.semantic_converter.from_python_to_arrow_schema(
                        self._meta_python_schema
                    )
                )

            assert self._cached_meta_arrow_schema is not None, (
                "Meta Arrow schema should be initialized by now"
            )

            if include_meta_columns is True:
                meta_schema = self._cached_meta_arrow_schema
            elif isinstance(include_meta_columns, Collection):
                # Filter meta schema by prefix matching
                matched_fields = [
                    field
                    for field in self._cached_meta_arrow_schema
                    if any(
                        field.name.startswith(prefix) for prefix in include_meta_columns
                    )
                ]
                if matched_fields:
                    meta_schema = pa.schema(matched_fields)
                else:
                    meta_schema = None
            else:
                meta_schema = None

            if meta_schema is not None:
                all_schemas.append(meta_schema)

        return arrow_utils.join_arrow_schemas(*all_schemas)

    def content_hash(self) -> str:
        """
        Calculate and return content hash of the datagram.
        Only includes data columns, not meta columns or context.

        Returns:
            Hash string of the datagram content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self._data_context.arrow_hasher.hash_table(
                self.as_table(include_meta_columns=False, include_context=False),
                prefix_hasher_id=True,
            )
        return self._cached_content_hash

    # 4. Format Conversions (Export)
    def as_dict(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation of the datagram.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context key

        Returns:
            Dictionary representation
        """
        result_dict = dict(self._data)  # Start with user data

        # Add context if requested
        if include_context:
            result_dict[constants.CONTEXT_KEY] = self._data_context.context_key

        # Add meta columns if requested
        if include_meta_columns and self._meta_data:
            if include_meta_columns is True:
                # Include all meta columns
                result_dict.update(self._meta_data)
            elif isinstance(include_meta_columns, Collection):
                # Include only meta columns matching prefixes
                filtered_meta_data = {
                    k: v
                    for k, v in self._meta_data.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
                result_dict.update(filtered_meta_data)

        return result_dict

    def _get_meta_arrow_table(self) -> pa.Table:
        if self._cached_meta_table is None:
            arrow_schema = self._get_meta_arrow_schema()
            self._cached_meta_table = pa.Table.from_pylist(
                [self._meta_data],
                schema=arrow_schema,
            )
        assert self._cached_meta_table is not None, (
            "Meta Arrow table should be initialized by now"
        )
        return self._cached_meta_table

    def _get_meta_arrow_schema(self) -> pa.Schema:
        if self._cached_meta_arrow_schema is None:
            self._cached_meta_arrow_schema = (
                self.semantic_converter.from_python_to_arrow_schema(
                    self._meta_python_schema
                )
            )
        assert self._cached_meta_arrow_schema is not None, (
            "Meta Arrow schema should be initialized by now"
        )
        return self._cached_meta_arrow_schema

    def as_table(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Table:
        """
        Convert the datagram to an Arrow table.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include the context column

        Returns:
            Arrow table representation
        """
        # Build data table (cached)
        if self._cached_data_table is None:
            self._cached_data_table = self.semantic_converter.from_python_to_arrow(
                self._data,
                self._data_python_schema,
            )
        assert self._cached_data_table is not None, (
            "Data Arrow table should be initialized by now"
        )
        result_table = self._cached_data_table

        # Add context if requested
        if include_context:
            result_table = result_table.append_column(
                constants.CONTEXT_KEY,
                pa.array([self._data_context.context_key], type=pa.large_string()),
            )

        # Add meta columns if requested
        meta_table = None
        if include_meta_columns and self._meta_data:
            meta_table = self._get_meta_arrow_table()
            # Select appropriate meta columns
            if isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                matched_cols = [
                    col
                    for col in self._meta_data.keys()
                    if any(col.startswith(prefix) for prefix in include_meta_columns)
                ]
                if matched_cols:
                    meta_table = meta_table.select(matched_cols)
                else:
                    meta_table = None

            # Combine tables if we have meta columns to add
            if meta_table is not None:
                result_table = arrow_utils.hstack_tables(result_table, meta_table)

        return result_table

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get meta column value with optional default.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix).
            default: Value to return if meta column doesn't exist.

        Returns:
            Meta column value if exists, otherwise the default value.
        """
        # Handle both prefixed and unprefixed keys
        if not key.startswith(constants.META_PREFIX):
            key = constants.META_PREFIX + key

        return self._meta_data.get(key, default)

    def with_meta_columns(self, **meta_updates: DataValue) -> "DictDatagram":
        """
        Create a new DictDatagram with updated meta columns.
        Maintains immutability by returning a new instance.

        Args:
            **meta_updates: Meta column updates (keys will be prefixed with {orcapod.META_PREFIX} ('__') if needed)

        Returns:
            New DictDatagram instance
        """
        # Prefix the keys and prepare updates
        prefixed_updates = {}
        for k, v in meta_updates.items():
            if not k.startswith(constants.META_PREFIX):
                k = constants.META_PREFIX + k
            prefixed_updates[k] = v

        # Start with existing meta data
        new_meta_data = dict(self._meta_data)
        new_meta_data.update(prefixed_updates)

        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(new_meta_data)  # Meta data

        return DictDatagram(
            data=full_data,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

    def drop_meta_columns(
        self, *keys: str, ignore_missing: bool = False
    ) -> "DictDatagram":
        """
        Create a new DictDatagram with specified meta columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *keys: Meta column keys to drop (with or without {orcapod.META_PREFIX} ('__') prefix)
            ignore_missing: If True, ignore missing meta columns without raising an error.

        Raises:
            KeyError: If any specified meta column to drop doesn't exist and ignore_missing=False.

        Returns:
            New DictDatagram instance without specified meta columns
        """
        # Normalize keys to have prefixes
        prefixed_keys = set()
        for key in keys:
            if not key.startswith(constants.META_PREFIX):
                key = constants.META_PREFIX + key
            prefixed_keys.add(key)

        missing_keys = prefixed_keys - set(self._meta_data.keys())
        if missing_keys and not ignore_missing:
            raise KeyError(
                f"Following meta columns do not exist and cannot be dropped: {sorted(missing_keys)}"
            )

        # Filter out specified meta columns
        new_meta_data = {
            k: v for k, v in self._meta_data.items() if k not in prefixed_keys
        }

        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(new_meta_data)  # Filtered meta data

        return DictDatagram(
            data=full_data,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

    # 6. Data Column Operations
    def select(self, *column_names: str) -> "DictDatagram":
        """
        Create a new DictDatagram with only specified data columns.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to keep

        Returns:
            New DictDatagram instance with only specified data columns
        """
        # Validate columns exist
        missing_cols = set(column_names) - set(self._data.keys())
        if missing_cols:
            raise KeyError(f"Columns not found: {missing_cols}")

        # Keep only specified data columns
        new_data = {k: v for k, v in self._data.items() if k in column_names}

        # Reconstruct full data dict for new instance
        full_data = new_data  # Selected user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return DictDatagram(
            data=full_data,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

    def drop(self, *column_names: str, ignore_missing: bool = False) -> "DictDatagram":
        """
        Create a new DictDatagram with specified data columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to drop

        Returns:
            New DictDatagram instance without specified data columns
        """
        # Filter out specified data columns
        missing = set(column_names) - set(self._data.keys())
        if missing and not ignore_missing:
            raise KeyError(
                f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
            )

        new_data = {k: v for k, v in self._data.items() if k not in column_names}

        if not new_data:
            raise ValueError("Cannot drop all data columns")

        # Reconstruct full data dict for new instance
        full_data = new_data  # Filtered user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return DictDatagram(
            data=full_data,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

    def rename(self, column_mapping: Mapping[str, str]) -> "DictDatagram":
        """
        Create a new DictDatagram with data columns renamed.
        Maintains immutability by returning a new instance.

        Args:
            column_mapping: Mapping from old column names to new column names

        Returns:
            New DictDatagram instance with renamed data columns
        """
        # Rename data columns according to mapping, preserving original types
        new_data = {}
        for old_name, value in self._data.items():
            new_name = column_mapping.get(old_name, old_name)
            new_data[new_name] = value

        # Handle typespec updates for renamed columns
        new_typespec = None
        if self._data_python_schema:
            existing_typespec = dict(self._data_python_schema)

            # Rename types according to column mapping
            renamed_typespec = {}
            for old_name, old_type in existing_typespec.items():
                new_name = column_mapping.get(old_name, old_name)
                renamed_typespec[new_name] = old_type

            new_typespec = renamed_typespec

        # Reconstruct full data dict for new instance
        full_data = new_data  # Renamed user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return DictDatagram(
            data=full_data,
            typespec=new_typespec,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

    def update(self, **updates: DataValue) -> "DictDatagram":
        """
        Create a new DictDatagram with existing column values updated.
        Maintains immutability by returning a new instance.

        Args:
            **updates: Column names and their new values (columns must exist)

        Returns:
            New DictDatagram instance with updated values

        Raises:
            KeyError: If any column doesn't exist (use with_columns() to add new columns)
        """
        if not updates:
            return self

        # Error if any column doesn't exist
        missing_columns = set(updates.keys()) - set(self._data.keys())
        if missing_columns:
            raise KeyError(
                f"Columns not found: {sorted(missing_columns)}. "
                f"Use with_columns() to add new columns."
            )

        # Update existing columns
        new_data = dict(self._data)
        new_data.update(updates)

        # Reconstruct full data dict for new instance
        full_data = new_data  # Updated user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return DictDatagram(
            data=full_data,
            semantic_converter=self.semantic_converter,  # Keep existing converter
            data_context=self._data_context,
        )

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> "DictDatagram":
        """
        Create a new DictDatagram with new data columns added.
        Maintains immutability by returning a new instance.

        Args:
            column_updates: New data columns as a mapping
            column_types: Optional type specifications for new columns
            **kwargs: New data columns as keyword arguments

        Returns:
            New DictDatagram instance with new data columns added

        Raises:
            ValueError: If any column already exists (use update() instead)
        """
        # Combine explicit updates with kwargs

        if not updates:
            return self

        # Error if any column already exists
        existing_overlaps = set(updates.keys()) & set(self._data.keys())
        if existing_overlaps:
            raise ValueError(
                f"Columns already exist: {sorted(existing_overlaps)}. "
                f"Use update() to modify existing columns."
            )

        # Update user data with new columns
        new_data = dict(self._data)
        new_data.update(updates)

        # Create updated typespec - handle None values by defaulting to str
        typespec = self.types()
        if column_types is not None:
            typespec.update(column_types)

        new_typespec = tsutils.get_typespec_from_dict(
            new_data,
            typespec=typespec,
        )

        # Reconstruct full data dict for new instance
        full_data = new_data  # Updated user data
        full_data.update(self._meta_data)  # Keep existing meta data

        return DictDatagram(
            data=full_data,
            typespec=new_typespec,
            # semantic converter needs to be rebuilt for new columns
            data_context=self._data_context,
        )

    # 7. Context Operations
    def with_context_key(self, new_context_key: str) -> "DictDatagram":
        """
        Create a new DictDatagram with a different data context key.
        Maintains immutability by returning a new instance.

        Args:
            new_context_key: New data context key string

        Returns:
            New DictDatagram instance with new context
        """
        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(self._meta_data)  # Meta data

        return DictDatagram(
            data=full_data,
            data_context=new_context_key,  # New context
            # Note: semantic_converter will be rebuilt for new context
        )

    # 8. Utility Operations
    def copy(self) -> Self:
        """
        Create a shallow copy of the datagram.

        Returns a new datagram instance with the same data and cached values.
        This is more efficient than reconstructing from scratch when you need
        an identical datagram instance.

        Returns:
            New DictDatagram instance with copied data and caches.
        """
        # Reconstruct full data dict for new instance
        full_data = dict(self._data)  # User data
        full_data.update(self._meta_data)  # Meta data

        new_datagram = self.__class__(
            full_data,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

        # Copy caches
        new_datagram._cached_data_table = self._cached_data_table
        new_datagram._cached_meta_table = self._cached_meta_table
        new_datagram._cached_content_hash = self._cached_content_hash
        new_datagram._cached_data_arrow_schema = self._cached_data_arrow_schema
        new_datagram._cached_meta_arrow_schema = self._cached_meta_arrow_schema

        return new_datagram

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.
        """
        return str(self._data)

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information including
        data columns, meta columns count, and context for debugging purposes.

        Returns:
            Detailed representation with type and metadata information.
        """
        meta_count = len(self.meta_columns)
        context_key = self.data_context_key

        return (
            f"DictDatagram("
            f"data={self._data}, "
            f"meta_columns={meta_count}, "
            f"context='{context_key}'"
            f")"
        )


class ArrowDatagram(BaseDatagram):
    """
    Immutable datagram implementation using PyArrow Table as storage backend.

    This implementation provides high-performance columnar data operations while
    maintaining the datagram interface. It efficiently handles type conversions,
    semantic processing, and interoperability with Arrow-based tools.

    The underlying table is split into separate components:
    - Data table: Primary business data columns
    - Meta table: Internal system metadata with {orcapod.META_PREFIX} ('__') prefixes
    - Context table: Data context information with {orcapod.CONTEXT_KEY}

    Future Packet subclass will also handle:
    - Source info: Data provenance with {orcapod.SOURCE_PREFIX} ('_source_') prefixes

    When exposing to external tools, semantic types are encoded as
    `_{semantic_type}_` prefixes (_path_config_file, _id_user_name).

    All operations return new instances, preserving immutability.

    Example:
        >>> table = pa.Table.from_pydict({
        ...     "user_id": [123],
        ...     "name": ["Alice"],
        ...     "__pipeline_version": ["v2.1.0"],
        ...     "{orcapod.CONTEXT_KEY}": ["financial_v1"]
        ... })
        >>> datagram = ArrowDatagram(table)
        >>> updated = datagram.update(name="Alice Smith")
    """

    def __init__(
        self,
        table: pa.Table,
        semantic_converter: SemanticConverter | None = None,
        data_context: str | DataContext | None = None,
    ) -> None:
        """
        Initialize ArrowDatagram from PyArrow Table.

        Args:
            table: PyArrow Table containing the data. Must have exactly one row.
            semantic_converter: Optional converter for semantic type handling.
                If None, will be created based on the data context and table schema.
            data_context: Context key string or DataContext object.
                If None and table contains context column, will extract from table.

        Raises:
            ValueError: If table doesn't contain exactly one row.

        Note:
            The input table is automatically split into data, meta, and context
            components based on column naming conventions.
        """
        # Validate table has exactly one row for datagram
        if len(table) != 1:
            raise ValueError(
                "Table must contain exactly one row to be a valid datagram."
            )

        # Split table into data, meta, and context components
        context_columns = [constants.CONTEXT_KEY]
        meta_columns = [
            col for col in table.column_names if col.startswith(constants.META_PREFIX)
        ]

        # Extract context table if present
        if constants.CONTEXT_KEY in table.column_names and data_context is None:
            context_table = table.select([constants.CONTEXT_KEY])
            data_context = context_table[constants.CONTEXT_KEY].to_pylist()[0]

        # Initialize base class with data context
        super().__init__(data_context)

        # Split table into components
        self._data_table = table.drop(context_columns + meta_columns)
        self._meta_table = table.select(meta_columns) if meta_columns else None
        if len(self._data_table.column_names) == 0:
            raise ValueError("Data table must contain at least one data column.")

        # Create semantic converter
        if semantic_converter is None:
            semantic_converter = SemanticConverter.from_semantic_schema(
                schemas.SemanticSchema.from_arrow_schema(
                    self._data_table.schema,
                    self._data_context.semantic_type_registry,
                )
            )
        self._semantic_converter = semantic_converter

        # Create data context table
        data_context_schema = pa.schema({constants.CONTEXT_KEY: pa.large_string()})
        self._data_context_table = pa.Table.from_pylist(
            [{constants.CONTEXT_KEY: self._data_context.context_key}],
            schema=data_context_schema,
        )

        # Initialize caches
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_python_dict: dict[str, DataValue] | None = None
        self._cached_meta_python_schema: schemas.PythonSchema | None = None
        self._cached_content_hash: str | None = None

    # 1. Core Properties (Identity & Structure)
    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names."""
        if self._meta_table is None:
            return ()
        return tuple(self._meta_table.column_names)

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """Get data column value by key."""
        if key not in self._data_table.column_names:
            raise KeyError(f"Data column '{key}' not found")

        return self._data_table[key].to_pylist()[0]

    def __contains__(self, key: str) -> bool:
        """Check if data column exists."""
        return key in self._data_table.column_names

    def __iter__(self) -> Iterator[str]:
        """Iterate over data column names."""
        return iter(self._data_table.column_names)

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """Get data column value with default."""
        if key in self._data_table.column_names:
            return self.as_dict()[key]
        return default

    # 3. Structural Information
    def keys(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> tuple[str, ...]:
        """Return tuple of column names."""
        # Start with data columns
        result_keys = list(self._data_table.column_names)

        # Add context if requested
        if include_context:
            result_keys.append(constants.CONTEXT_KEY)

        # Add meta columns if requested
        if include_meta_columns:
            if include_meta_columns is True:
                result_keys.extend(self.meta_columns)
            elif isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                filtered_meta_cols = [
                    col
                    for col in self.meta_columns
                    if any(col.startswith(prefix) for prefix in include_meta_columns)
                ]
                result_keys.extend(filtered_meta_cols)

        return tuple(result_keys)

    def types(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> schemas.PythonSchema:
        """
        Return Python schema for the datagram.

        Args:
            include_meta_columns: Whether to include meta column types.
                - True: include all meta column types
                - Collection[str]: include meta column types matching these prefixes
                - False: exclude meta column types
            include_context: Whether to include context type

        Returns:
            Python schema
        """
        # Get data schema (cached)
        if self._cached_python_schema is None:
            self._cached_python_schema = (
                self._semantic_converter.from_arrow_to_python_schema(
                    self._data_table.schema
                )
            )

        schema = dict(self._cached_python_schema)

        # Add context if requested
        if include_context:
            schema[constants.CONTEXT_KEY] = str

        # Add meta schema if requested
        if include_meta_columns and self._meta_table is not None:
            if self._cached_meta_python_schema is None:
                self._cached_meta_python_schema = (
                    self._semantic_converter.from_arrow_to_python_schema(
                        self._meta_table.schema
                    )
                )
            meta_schema = dict(self._cached_meta_python_schema)
            if include_meta_columns is True:
                schema.update(meta_schema)
            elif isinstance(include_meta_columns, Collection):
                filtered_meta_schema = {
                    k: v
                    for k, v in meta_schema.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
                schema.update(filtered_meta_schema)

        return schemas.PythonSchema(schema)

    def arrow_schema(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Schema:
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_meta_columns: Whether to include meta columns in the schema.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context column in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        all_schemas = [self._data_table.schema]

        # Add context schema if requested
        if include_context:
            # TODO: reassess the efficiency of this approach
            all_schemas.append(self._data_context_table.schema)

        # Add meta schema if requested
        if include_meta_columns and self._meta_table is not None:
            if include_meta_columns is True:
                meta_schema = self._meta_table.schema
            elif isinstance(include_meta_columns, Collection):
                # Filter meta schema by prefix matching
                matched_fields = [
                    field
                    for field in self._meta_table.schema
                    if any(
                        field.name.startswith(prefix) for prefix in include_meta_columns
                    )
                ]
                if matched_fields:
                    meta_schema = pa.schema(matched_fields)
                else:
                    meta_schema = None
            else:
                meta_schema = None

            if meta_schema is not None:
                all_schemas.append(meta_schema)

        return arrow_utils.join_arrow_schemas(*all_schemas)

    def content_hash(self) -> str:
        """
        Calculate and return content hash of the datagram.
        Only includes data columns, not meta columns or context.

        Returns:
            Hash string of the datagram content
        """
        if self._cached_content_hash is None:
            self._cached_content_hash = self._data_context.arrow_hasher.hash_table(
                self._data_table,
                prefix_hasher_id=True,
            )
        return self._cached_content_hash

    # 4. Format Conversions (Export)
    def as_dict(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation of the datagram.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include context key

        Returns:
            Dictionary representation
        """
        # Get data dict (cached)
        if self._cached_python_dict is None:
            self._cached_python_dict = self._semantic_converter.from_arrow_to_python(
                self._data_table
            )[0]

        result_dict = dict(self._cached_python_dict)

        # Add context if requested
        if include_context:
            result_dict[constants.CONTEXT_KEY] = self._data_context.context_key

        # Add meta data if requested
        if include_meta_columns and self._meta_table is not None:
            if include_meta_columns is True:
                meta_dict = self._meta_table.to_pylist()[0]
            elif isinstance(include_meta_columns, Collection):
                meta_dict = self._meta_table.to_pylist()[0]
                # Include only meta columns matching prefixes
                meta_dict = {
                    k: v
                    for k, v in meta_dict.items()
                    if any(k.startswith(prefix) for prefix in include_meta_columns)
                }
            if meta_dict is not None:
                result_dict.update(meta_dict)

        return result_dict

    def as_table(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> pa.Table:
        """
        Convert the datagram to an Arrow table.

        Args:
            include_meta_columns: Whether to include meta columns.
                - True: include all meta columns
                - Collection[str]: include meta columns matching these prefixes
                - False: exclude meta columns
            include_context: Whether to include the context column

        Returns:
            Arrow table representation
        """
        all_tables = [self._data_table]

        # Add context if requested
        if include_context:
            all_tables.append(self._data_context_table)

        # Add meta columns if requested
        if include_meta_columns and self._meta_table is not None:
            meta_table = None
            if include_meta_columns is True:
                meta_table = self._meta_table
            elif isinstance(include_meta_columns, Collection):
                # Filter meta columns by prefix matching
                matched_cols = [
                    col
                    for col in self._meta_table.column_names
                    if any(col.startswith(prefix) for prefix in include_meta_columns)
                ]
                if matched_cols:
                    meta_table = self._meta_table.select(matched_cols)
                else:
                    meta_table = None

            if meta_table is not None:
                all_tables.append(meta_table)

        return arrow_utils.hstack_tables(*all_tables)

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get a meta column value.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix)
            default: Default value if not found

        Returns:
            Meta column value
        """
        if self._meta_table is None:
            return default

        # Handle both prefixed and unprefixed keys
        if not key.startswith(constants.META_PREFIX):
            key = constants.META_PREFIX + key

        if key not in self._meta_table.column_names:
            return default

        return self._meta_table[key].to_pylist()[0]

    def with_meta_columns(self, **meta_updates: DataValue) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with updated meta columns.
        Maintains immutability by returning a new instance.

        Args:
            **meta_updates: Meta column updates (keys will be prefixed with {orcapod.META_PREFIX} ('__') if needed)

        Returns:
            New ArrowDatagram instance
        """
        # Prefix the keys and prepare updates
        prefixed_updates = {}
        for k, v in meta_updates.items():
            if not k.startswith(constants.META_PREFIX):
                k = constants.META_PREFIX + k
            prefixed_updates[k] = v

        # Start with existing meta data
        meta_dict = {}
        if self._meta_table is not None:
            meta_dict = self._meta_table.to_pylist()[0]

        # Apply updates
        meta_dict.update(prefixed_updates)

        # Create new meta table
        new_meta_table = pa.Table.from_pylist([meta_dict]) if meta_dict else None

        # Combine all tables for reconstruction
        combined_table = self._data_table
        if new_meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, new_meta_table)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def drop_meta_columns(
        self, *keys: str, ignore_missing: bool = True
    ) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with specified meta columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *keys: Meta column keys to drop (with or without {orcapod.META_PREFIX} ('__') prefix)

        Returns:
            New ArrowDatagram instance without specified meta columns
        """
        if self._meta_table is None:
            return self  # No meta columns to drop

        # Normalize keys to have prefixes
        prefixed_keys = set()
        for key in keys:
            if not key.startswith(constants.META_PREFIX):
                key = constants.META_PREFIX + key
            prefixed_keys.add(key)

        missing_keys = prefixed_keys - set(self._meta_table.column_names)
        if missing_keys and not ignore_missing:
            raise KeyError(
                f"Following meta columns do not exist and cannot be dropped: {sorted(missing_keys)}"
            )

        # Filter meta columns
        remaining_cols = [
            col for col in self._meta_table.column_names if col not in prefixed_keys
        ]

        # Create new meta table
        new_meta_table = (
            self._meta_table.select(remaining_cols) if remaining_cols else None
        )

        # Combine tables for reconstruction
        combined_table = self._data_table
        if new_meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, new_meta_table)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    # 6. Data Column Operations
    def select(self, *column_names: str) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with only specified data columns.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to keep

        Returns:
            New ArrowDatagram instance with only specified data columns
        """
        # Validate columns exist
        missing_cols = set(column_names) - set(self._data_table.column_names)
        if missing_cols:
            raise ValueError(f"Columns not found: {missing_cols}")

        new_data_table = self._data_table.select(list(column_names))

        # Combine with meta table for reconstruction
        combined_table = new_data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def drop(self, *column_names: str, ignore_missing: bool = False) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with specified data columns dropped.
        Maintains immutability by returning a new instance.

        Args:
            *column_names: Data column names to drop

        Returns:
            New ArrowDatagram instance without specified data columns
        """

        # Filter out specified data columns
        missing = set(column_names) - set(self._data_table.column_names)
        if missing and not ignore_missing:
            raise KeyError(
                f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
            )

        # Filter data columns
        remaining_cols = [
            col for col in self._data_table.column_names if col not in column_names
        ]

        if not remaining_cols:
            raise ValueError("Cannot drop all data columns")

        new_data_table = self._data_table.select(remaining_cols)

        # Combine with meta table for reconstruction
        combined_table = new_data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def rename(self, column_mapping: Mapping[str, str]) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with data columns renamed.
        Maintains immutability by returning a new instance.

        Args:
            column_mapping: Mapping from old column names to new column names

        Returns:
            New ArrowDatagram instance with renamed data columns
        """
        # Create new schema with renamed fields, preserving original types
        new_fields = []
        for field in self._data_table.schema:
            old_name = field.name
            new_name = column_mapping.get(old_name, old_name)
            new_field = pa.field(new_name, field.type)
            new_fields.append(new_field)

        # Create new data table with renamed columns
        new_schema = pa.schema(new_fields)
        new_data_table = self._data_table.rename_columns(
            [column_mapping.get(name, name) for name in self._data_table.column_names]
        ).cast(new_schema)

        # Combine with meta table for reconstruction
        combined_table = new_data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def update(self, **updates: DataValue) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with specific column values updated.

        Args:
            **updates: Column names and their new values

        Returns:
            New ArrowDatagram instance with updated values

        Raises:
            KeyError: If any specified column doesn't exist

        Example:
            # Convert relative path to absolute path
            updated = datagram.update(file_path="/absolute/path/to/file.txt")

            # Update multiple values
            updated = datagram.update(status="processed", file_path="/new/path")
        """
        # Only update if there are columns to update
        if not updates:
            return self

        # Validate all columns exist
        missing_cols = set(updates.keys()) - set(self._data_table.column_names)
        if missing_cols:
            raise KeyError(
                f"Only existing columns can be updated. Following columns were not found: {sorted(missing_cols)}"
            )

        updates_typespec = schemas.PythonSchema(
            {k: v for k, v in self.types().items() if k in updates}
        )

        update_table = self._semantic_converter.from_python_to_arrow(
            updates, updates_typespec
        )
        all_tables = [self._data_table.drop(list(updates.keys())), update_table]

        if self._meta_table is not None:
            all_tables.append(self._meta_table)

        return ArrowDatagram(
            table=arrow_utils.hstack_tables(*all_tables),
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with new data columns added.
        Maintains immutability by returning a new instance.

        Args:
            column_updates: New data columns as a mapping
            column_types: Optional type specifications for new columns
            **kwargs: New data columns as keyword arguments

        Returns:
            New ArrowDatagram instance with new data columns added

        Raises:
            ValueError: If any column already exists (use update() instead)
        """
        # Combine explicit updates with kwargs

        if not updates:
            return self

        # Error if any column already exists
        existing_overlaps = set(updates.keys()) & set(self._data_table.column_names)
        if existing_overlaps:
            raise ValueError(
                f"Columns already exist: {sorted(existing_overlaps)}. "
                f"Use update() to modify existing columns."
            )

        # TODO: consider simplifying this conversion logic
        typespec = typespec_utils.get_typespec_from_dict(updates, column_types)

        updates_converter = SemanticConverter.from_semantic_schema(
            schemas.SemanticSchema.from_typespec(
                typespec, self._data_context.semantic_type_registry
            )
        )
        # TODO: cleanup the handling of typespec python schema and various conversion points
        new_data_table = updates_converter.from_python_to_arrow(updates, typespec)

        # Combine with meta table for reconstruction
        combined_table = new_data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        # prepare the joined converter
        total_converter = self._semantic_converter.join(updates_converter)

        return ArrowDatagram(
            table=combined_table,
            semantic_converter=total_converter,
            data_context=self._data_context,
        )

    # 7. Context Operations
    def with_context_key(self, new_context_key: str) -> "ArrowDatagram":
        """
        Create a new ArrowDatagram with a different data context key.
        Maintains immutability by returning a new instance.

        Args:
            new_context_key: New data context key string

        Returns:
            New ArrowDatagram instance with new context
        """
        # Combine all tables for reconstruction
        combined_table = self._data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        return ArrowDatagram(
            table=combined_table,
            data_context=new_context_key,
            # Note: semantic_converter will be rebuilt for new context
        )

    # 8. Utility Operations
    def copy(self) -> Self:
        """Return a copy of the datagram."""
        # Combine all tables for reconstruction
        combined_table = self._data_table
        if self._meta_table is not None:
            combined_table = arrow_utils.hstack_tables(combined_table, self._meta_table)

        new_datagram = self.__class__(
            combined_table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

        # Copy caches
        new_datagram._cached_python_schema = self._cached_python_schema
        new_datagram._cached_python_dict = self._cached_python_dict
        new_datagram._cached_content_hash = self._cached_content_hash

        return new_datagram

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.

        Example:
            >>> str(datagram)
            "{'user_id': 123, 'name': 'Alice'}"
            >>> print(datagram)
            {'user_id': 123, 'name': 'Alice'}
        """
        return str(self.as_dict())

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information including
        data columns, meta columns count, and context for debugging purposes.

        Returns:
            Detailed representation with type and metadata information.

        Example:
            >>> repr(datagram)
            "ArrowDatagram(data={'user_id': 123, 'name': 'Alice'}, meta_columns=2, context='std:v1.0.0:abc123')"
        """
        data_dict = self.as_dict()
        meta_count = len(self.meta_columns)
        context_key = self.data_context_key

        return (
            f"ArrowDatagram("
            f"data={data_dict}, "
            f"meta_columns={meta_count}, "
            f"context='{context_key}'"
            f")"
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
        data_context: str | DataContext | None = None,
    ) -> None:
        # normalize the data content and remove any source info keys
        data_only = {
            k: v for k, v in data.items() if not k.startswith(constants.SOURCE_PREFIX)
        }
        contained_source_info = {
            k.removeprefix(constants.SOURCE_PREFIX): v
            for k, v in data.items()
            if k.startswith(constants.SOURCE_PREFIX)
        }

        super().__init__(
            data_only,
            typespec=typespec,
            semantic_converter=semantic_converter,
            data_context=data_context,
        )

        self._source_info = {**contained_source_info, **(source_info or {})}
        self._cached_source_info_table: pa.Table | None = None
        self._cached_source_info_schema: pa.Schema | None = None

    @property
    def _source_info_schema(self) -> pa.Schema:
        if self._cached_source_info_schema is None:
            self._cached_source_info_schema = pa.schema(
                {
                    f"{constants.SOURCE_PREFIX}{k}": pa.large_string()
                    for k in self.keys()
                }
            )
        return self._cached_source_info_schema

    def as_table(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        table = super().as_table(
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_source:
            if self._cached_source_info_table is None:
                source_info_data = {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
                self._cached_source_info_table = pa.Table.from_pylist(
                    [source_info_data], schema=self._source_info_schema
                )
            assert self._cached_source_info_table is not None, (
                "Cached source info table should not be None"
            )
            # subselect the corresponding _source_info as the columns present in the data table
            source_info_table = self._cached_source_info_table.select(
                [
                    f"{constants.SOURCE_PREFIX}{k}"
                    for k in table.column_names
                    if k in self.keys()
                ]
            )
            table = arrow_utils.hstack_tables(table, source_info_table)
        return table

    def as_dict(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = super().as_dict(
            include_meta_columns=include_meta_columns, include_context=include_context
        )
        if include_source:
            for key, value in self.source_info().items():
                dict_copy[f"{constants.SOURCE_PREFIX}{key}"] = value
        return dict_copy

    def types(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> schemas.PythonSchema:
        """Return copy of the Python schema."""
        schema = super().types(
            include_meta_columns=include_meta_columns, include_context=include_context
        )
        if include_source:
            for key in self.keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = str
        return schema

    def arrow_schema(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> pa.Schema:
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_data_context: Whether to include data context column in the schema
            include_source: Whether to include source info columns in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        schema = super().arrow_schema(
            include_meta_columns=include_meta_columns, include_context=include_context
        )
        if include_source:
            return arrow_utils.join_arrow_schemas(schema, self._source_info_schema)
        return schema

    def as_datagram(
        self,
        include_meta_columns: bool | Collection[str] = False,
        include_source: bool = False,
    ) -> DictDatagram:
        """
        Convert the packet to a DictDatagram.

        Args:
            include_source: Whether to include source info fields

        Returns:
            DictDatagram representation of the packet
        """
        data = self.as_dict(
            include_meta_columns=include_meta_columns, include_source=include_source
        )
        typespec = self.types(include_source=include_source)
        return DictDatagram(
            data,
            typespec=typespec,
            semantic_converter=self.semantic_converter,
            data_context=self._data_context,
        )

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
        data_context: str | DataContext | None = None,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowTag should only contain a single row, "
                "as it represents a single tag."
            )
        super().__init__(
            table=table,
            semantic_converter=semantic_converter,
            data_context=data_context,
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
        data_context: str | DataContext | None = None,
    ) -> None:
        if len(data) != 1:
            raise ValueError(
                "ArrowPacket should only contain a single row, "
                "as it represents a single packet."
            )
        if source_info is None:
            source_info = {}

        # normalize the table to ensure it has the expected source_info columns
        data_table, prefixed_tables = arrow_utils.prepare_prefixed_columns(
            data,
            {constants.SOURCE_PREFIX: source_info},
            exclude_columns=[constants.CONTEXT_KEY],
        )
        self._source_info_table = prefixed_tables[constants.SOURCE_INFO_PREFIX]

        super().__init__(
            data_table,
            semantic_converter=semantic_converter,
            data_context=data_context,
        )

        self._cached_source_info: dict[str, str | None] | None = None
        self._cached_python_schema: schemas.PythonSchema | None = None
        self._cached_content_hash: str | None = None

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
    ) -> pa.Table:
        table = super().as_table(include_data_context=include_data_context)
        if include_source:
            # add source_info only for existing data columns
            table = arrow_utils.hstack_tables(
                table,
                self._source_info_table.select(
                    [
                        f"{constants.SOURCE_INFO_PREFIX}{c}"
                        for c in table.column_names
                        if c in self.keys()
                    ]
                ),
            )
        return table

    def types(
        self, include_data_context: bool = False, include_source: bool = False
    ) -> schemas.PythonSchema:
        """Return copy of the Python schema."""
        schema = super().types(include_data_context=include_data_context)
        if include_source:
            for key in self.keys():
                schema[f"{constants.SOURCE_INFO_PREFIX}{key}"] = str
        return schema

    def arrow_schema(
        self, include_data_context: bool = False, include_source: bool = False
    ) -> pa.Schema:
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_data_context: Whether to include data context column in the schema
            include_source: Whether to include source info columns in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        schema = super().arrow_schema(include_data_context=include_data_context)
        if include_source:
            return arrow_utils.join_arrow_schemas(
                schema, self._source_info_table.schema
            )
        return schema

    def as_dict(
        self, include_data_context: bool = False, include_source: bool = False
    ) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        return_dict = super().as_dict(include_data_context=include_data_context)
        if include_source:
            return_dict.update(
                {
                    f"{constants.SOURCE_INFO_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
            )
        return return_dict

    def as_datagram(self, include_source: bool = False) -> ArrowDatagram:
        table = self.as_table(include_source=include_source)
        return ArrowDatagram(
            table,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        if self._cached_source_info is None:
            self._cached_source_info = {
                k.removeprefix(constants.SOURCE_INFO_PREFIX): v
                for k, v in self._source_info_table.to_pylist()[0].items()
            }
        return self._cached_source_info.copy()

    def copy(self) -> Self:
        # TODO: restructure copy to allow for better inheritance and expansion
        new_packet = self.__class__(
            self.as_table(),
            self.source_info(),
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )
        new_packet._cached_source_info = self._cached_source_info
        new_packet._cached_python_dict = self._cached_python_dict
        new_packet._cached_python_schema = self._cached_python_schema
        new_packet._cached_content_hash = self._cached_content_hash

        return new_packet


# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[dp.Tag, Collection[dp.Packet]]
"""Type alias for a batch: a tuple containing a tag and collection of packets."""
