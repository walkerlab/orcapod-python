from collections.abc import Collection, Iterator, Mapping
from datetime import datetime
from typing import Any, ContextManager, Protocol, Self, TYPE_CHECKING
from orcapod.protocols.hashing_protocols import ContentIdentifiable
from orcapod.types import DataValue, TypeSpec

if TYPE_CHECKING:
    import pyarrow as pa


class Datagram(Protocol):
    """
    Protocol for immutable datagram containers in Orcapod.

    Datagrams are the fundamental units of data that flow through the system.
    They provide a unified interface for data access, conversion, and manipulation,
    ensuring consistent behavior across different storage backends (dict, Arrow table, etc.).

    Each datagram contains:
    - **Data columns**: The primary business data (user_id, name, etc.)
    - **Meta columns**: Internal system metadata with {orcapod.META_PREFIX} ('__') prefixes (__processed_at, etc.)
    - **Context column**: Data context information ({orcapod.CONTEXT_KEY})

    Future Packet subclass will also include:
    - **Source info columns**: Data provenance with {orcapod.SOURCE_PREFIX} ('_source_') prefixes (_source_user_id, etc.)

    When exposing to external tools without field metadata support, semantic types
    are encoded as `_{semantic_type}_` prefixes (_path_config_file, _id_user_name, etc.).

    All operations are immutable - methods return new datagram instances rather than
    modifying existing ones.

    Example:
        >>> datagram = DictDatagram({"user_id": 123, "name": "Alice"})
        >>> updated = datagram.update(name="Alice Smith")
        >>> filtered = datagram.select("user_id", "name")
        >>> table = datagram.as_table()
    """

    # 1. Core Properties (Identity & Structure)
    @property
    def data_context_key(self) -> str:
        """
        Return the data context key for this datagram.

        This key identifies the semantic type registry, arrow hasher, and other
        contextual information needed to properly interpret and work with this
        datagram across various operations (storage, visualization, processing, etc.).

        Returns:
            str: Context key for proper datagram interpretation
        """
        ...

    @property
    def meta_columns(self) -> tuple[str, ...]:
        """Return tuple of meta column names (with {orcapod.META_PREFIX} ('__') prefix)."""
        ...

    # 2. Dict-like Interface (Data Access)
    def __getitem__(self, key: str) -> DataValue:
        """
        Get data column value by key.

        Provides dict-like access to data columns only. Meta columns
        are not accessible through this method (use `get_meta_value()` instead).

        Args:
            key: Data column name.

        Returns:
            The value stored in the specified data column.

        Raises:
            KeyError: If the column doesn't exist in data columns.

        Example:
            >>> datagram["user_id"]
            123
            >>> datagram["name"]
            'Alice'
        """
        ...

    def __contains__(self, key: str) -> bool:
        """
        Check if data column exists.

        Args:
            key: Column name to check.

        Returns:
            True if column exists in data columns, False otherwise.

        Example:
            >>> "user_id" in datagram
            True
            >>> "nonexistent" in datagram
            False
        """
        ...

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over data column names.

        Provides for-loop support over column names, enabling natural iteration
        patterns without requiring conversion to dict.

        Yields:
            Data column names in no particular order.

        Example:
            >>> for column in datagram:
            ...     value = datagram[column]
            ...     print(f"{column}: {value}")
        """
        ...

    def get(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get data column value with default fallback.

        Args:
            key: Data column name.
            default: Value to return if column doesn't exist.

        Returns:
            Column value if exists, otherwise the default value.

        Example:
            >>> datagram.get("user_id")
            123
            >>> datagram.get("missing", "default")
            'default'
        """
        ...

    # 3. Structural Information
    def keys(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> tuple[str, ...]:
        """
        Return tuple of column names.

        Provides access to column names with filtering options for different
        column types. Default returns only data column names.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Return only data column names (default)
                - True: Include all meta column names
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.

        Returns:
            Tuple of column names based on inclusion criteria.

        Example:
            >>> datagram.keys()  # Data columns only
            ('user_id', 'name', 'email')
            >>> datagram.keys(include_meta_columns=True)
            ('user_id', 'name', 'email', f'{orcapod.META_PREFIX}processed_at', f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_meta_columns=["pipeline"])
            ('user_id', 'name', 'email',f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_context=True)
            ('user_id', 'name', 'email', f'{orcapod.CONTEXT_KEY}')
        """
        ...

    def types(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> TypeSpec:
        """
        Return type specification mapping field names to Python types.

        The TypeSpec enables type checking and validation throughout the system.

        Args:
            include_meta_columns: Controls meta column type inclusion.
                - False: Exclude meta column types (default)
                - True: Include all meta column types
                - Collection[str]: Include meta column types matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context type.

        Returns:
            TypeSpec mapping field names to their Python types.

        Example:
            >>> datagram.types()
            {'user_id': <class 'int'>, 'name': <class 'str'>}
        """
        ...

    def arrow_schema(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> "pa.Schema":
        """
        Return PyArrow schema representation.

        The schema provides structured field and type information for efficient
        serialization and deserialization with PyArrow.

        Args:
            include_meta_columns: Controls meta column schema inclusion.
                - False: Exclude meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.

        Returns:
            PyArrow Schema describing the datagram structure.

        Example:
            >>> schema = datagram.arrow_schema()
            >>> schema.names
            ['user_id', 'name']
        """
        ...

    def content_hash(self) -> str:
        """
        Return deterministic hash of datagram content.

        The hash should reflect the data content, typically excluding meta columns
        and context. Used for caching, comparison, and deduplication. For exact details of
        hash computation, refer to the implementation in the specific datagram class/subclass.

        Returns:
            Deterministic content hash string.

        Note:
            Two datagrams with identical data columns will have the same hash,
            even if they differ in meta columns or context.

        Example:
            >>> datagram.content_hash()
            'sha256:abc123def456...'
        """
        ...

    # 4. Format Conversions (Export)
    def as_dict(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert datagram to dictionary format.

        Provides a simple key-value representation useful for debugging,
        serialization, and interop with dict-based APIs.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context key.
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.


        Returns:
            Dictionary with requested columns as key-value pairs.

        Example:
            >>> data = datagram.as_dict()  # {'user_id': 123, 'name': 'Alice'}
            >>> full_data = datagram.as_dict(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
        """
        ...

    def as_table(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
    ) -> "pa.Table":
        """
        Convert datagram to PyArrow Table format.

        Provides a standardized columnar representation suitable for analysis,
        processing, and interoperability with Arrow-based tools.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context column.
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.

        Returns:
            PyArrow Table with requested columns.

        Example:
            >>> table = datagram.as_table()  # Data columns only
            >>> full_table = datagram.as_table(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
            >>> filtered = datagram.as_table(include_meta_columns=["pipeline"])   # same as passing f"{orcapod.META_PREFIX}pipeline"
        """
        ...

    # TODO: add this back
    # def as_arrow_compatible_dict(
    #     self,
    #     include_all_info: bool = False,
    #     include_meta_columns: bool | Collection[str] = False,
    #     include_context: bool = False,
    # ) -> dict[str, Any]:
    #     """
    #     Return dictionary with values optimized for Arrow table conversion.

    #     This method returns a dictionary where values are in a form that can be
    #     efficiently converted to Arrow format using pa.Table.from_pylist().

    #     The key insight is that this avoids the expensive as_table() → concat pattern
    #     by providing values that are "Arrow-ready" while remaining in dict format
    #     for efficient batching.

    #     Implementation note: This may involve format conversions (e.g., Path objects
    #     to strings, datetime objects to ISO strings, etc.) to ensure compatibility
    #     with Arrow's expected input formats.

    #     Arrow table that results from pa.Table.from_pylist on the output of this should be accompanied
    #     with arrow_schema(...) with the same argument options to ensure that the schema matches the table.

    #     Args:
    #         include_all_info: Include all available information
    #         include_meta_columns: Controls meta column inclusion
    #         include_context: Whether to include context key

    #     Returns:
    #         Dictionary with values optimized for Arrow conversion

    #     Example:
    #         # Efficient batch conversion pattern
    #         arrow_dicts = [datagram.as_arrow_compatible_dict() for datagram in datagrams]
    #         schema = datagrams[0].arrow_schema()
    #         table = pa.Table.from_pylist(arrow_dicts, schema=schema)
    #     """
    #     ...

    # 5. Meta Column Operations
    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        """
        Get meta column value with optional default.

        Meta columns store operational metadata and use {orcapod.META_PREFIX} ('__') prefixes.
        This method handles both prefixed and unprefixed key formats.

        Args:
            key: Meta column key (with or without {orcapod.META_PREFIX} ('__') prefix).
            default: Value to return if meta column doesn't exist.

        Returns:
            Meta column value if exists, otherwise the default value.

        Example:
            >>> datagram.get_meta_value("pipeline_version")  # Auto-prefixed
            'v2.1.0'
            >>> datagram.get_meta_value("__pipeline_version")  # Already prefixed
            'v2.1.0'
            >>> datagram.get_meta_value("missing", "default")
            'default'
        """
        ...

    def with_meta_columns(self, **updates: DataValue) -> Self:
        """
        Create new datagram with updated meta columns.

        Adds or updates operational metadata while preserving all data columns.
        Keys are automatically prefixed with {orcapod.META_PREFIX} ('__') if needed.

        Args:
            **updates: Meta column updates as keyword arguments.

        Returns:
            New datagram instance with updated meta columns.

        Example:
            >>> tracked = datagram.with_meta_columns(
            ...     processed_by="pipeline_v2",
            ...     timestamp="2024-01-15T10:30:00Z"
            ... )
        """
        ...

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        """
        Create new datagram with specified meta columns removed.

        Args:
            *keys: Meta column keys to remove (prefixes optional).
            ignore_missing: If True, ignore missing columns without raising an error.


        Returns:
            New datagram instance without specified meta columns.

        Raises:
            KeryError: If any specified meta column to drop doesn't exist and ignore_missing=False.

        Example:
            >>> cleaned = datagram.drop_meta_columns("old_source", "temp_debug")
        """
        ...

    # 6. Data Column Operations
    def select(self, *column_names: str) -> Self:
        """
        Create new datagram with only specified data columns.

        Args:
            *column_names: Data column names to keep.


        Returns:
            New datagram instance with only specified data columns. All other columns including
            meta columns and context are preserved.

        Raises:
            KeyError: If any specified column doesn't exist.

        Example:
            >>> subset = datagram.select("user_id", "name", "email")
        """
        ...

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """
        Create new datagram with specified data columns removed. Note that this does not
        remove meta columns or context column. Refer to `drop_meta_columns()` for dropping
        specific meta columns. Context key column can never be dropped but a modified copy
        can be created with a different context key using `with_data_context()`.

        Args:
            *column_names: Data column names to remove.
            ignore_missing: If True, ignore missing columns without raising an error.

        Returns:
            New datagram instance without specified data columns.

        Raises:
            KeryError: If any specified column to drop doesn't exist and ignore_missing=False.

        Example:
            >>> filtered = datagram.drop("temp_field", "debug_info")
        """
        ...

    def rename(
        self,
        column_mapping: Mapping[str, str],
    ) -> Self:
        """
        Create new datagram with data columns renamed.

        Args:
            column_mapping: Mapping from old names to new names.

        Returns:
            New datagram instance with renamed data columns.

        Example:
            >>> renamed = datagram.rename(
            ...     {"old_id": "user_id", "old_name": "full_name"},
            ...     column_types={"user_id": int}
            ... )
        """
        ...

    def update(self, **updates: DataValue) -> Self:
        """
        Create new datagram with existing column values updated.

        Updates values in existing data columns. Will error if any specified
        column doesn't exist - use with_columns() to add new columns.

        Args:
            **updates: Column names and their new values.

        Returns:
            New datagram instance with updated values.

        Raises:
            KeyError: If any specified column doesn't exist.

        Example:
            >>> updated = datagram.update(
            ...     file_path="/new/absolute/path.txt",
            ...     status="processed"
            ... )
        """
        ...

    def with_columns(
        self,
        column_types: Mapping[str, type] | None = None,
        **updates: DataValue,
    ) -> Self:
        """
        Create new datagram with additional data columns.

        Adds new data columns to the datagram. Will error if any specified
        column already exists - use update() to modify existing columns.

        Args:
            column_types: Optional type specifications for new columns. If not provided, the column type is
                inferred from the provided values. If value is None, the column type defaults to `str`.
            **kwargs: New columns as keyword arguments.

        Returns:
            New datagram instance with additional data columns.

        Raises:
            ValueError: If any specified column already exists.

        Example:
            >>> expanded = datagram.with_columns(
            ...     status="active",
            ...     score=95.5,
            ...     column_types={"score": float}
            ... )
        """
        ...

    # 7. Context Operations
    def with_context_key(self, new_context_key: str) -> Self:
        """
        Create new datagram with different context key.

        Changes the semantic interpretation context while preserving all data.
        The context key affects how columns are processed and converted.

        Args:
            new_context_key: New context key string.

        Returns:
            New datagram instance with updated context key.

        Note:
            How the context is interpreted depends on the datagram implementation.
            Semantic processing may be rebuilt for the new context.

        Example:
            >>> financial_datagram = datagram.with_context_key("financial_v1")
        """
        ...

    # 8. Utility Operations
    def copy(self) -> Self:
        """
        Create a shallow copy of the datagram.

        Returns a new datagram instance with the same data and cached values.
        This is more efficient than reconstructing from scratch when you need
        an identical datagram instance.

        Returns:
            New datagram instance with copied data and caches.

        Example:
            >>> copied = datagram.copy()
            >>> copied is datagram  # False - different instance
            False
        """
        ...

    # 9. String Representations
    def __str__(self) -> str:
        """
        Return user-friendly string representation.

        Shows the datagram as a simple dictionary for user-facing output,
        messages, and logging. Only includes data columns for clean output.

        Returns:
            Dictionary-style string representation of data columns only.
        """
        ...

    def __repr__(self) -> str:
        """
        Return detailed string representation for debugging.

        Shows the datagram type and comprehensive information for debugging.

        Returns:
            Detailed representation with type and metadata information.
        """
        ...


class Tag(Datagram, Protocol):
    """
    Metadata associated with each data item in a stream.

    Tags carry contextual information about data packets as they flow through
    the computational graph. They are immutable and provide metadata that
    helps with:
    - Data lineage tracking
    - Grouping and aggregation operations
    - Temporal information (timestamps)
    - Source identification
    - Processing context

    Common examples include:
    - Timestamps indicating when data was created/processed
    - Source identifiers showing data origin
    - Processing metadata like batch IDs or session information
    - Grouping keys for aggregation operations
    - Quality indicators or confidence scores
    """

    pass


class Packet(Datagram, Protocol):
    """
    The actual data payload in a stream.

    Packets represent the core data being processed through the computational
    graph. Unlike Tags (which are metadata), Packets contain the actual
    information that computations operate on.

    Packets extend Datagram with additional capabilities for:
    - Source tracking and lineage
    - Content-based hashing for caching
    - Metadata inclusion for debugging

    The distinction between Tag and Packet is crucial for understanding
    data flow: Tags provide context, Packets provide content.
    """

    def keys(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> tuple[str, ...]:
        """
        Return tuple of column names.

        Provides access to column names with filtering options for different
        column types. Default returns only data column names.

        Args:
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.
            include_meta_columns: Controls meta column inclusion.
                - False: Return only data column names (default)
                - True: Include all meta column names
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.
            include_source: Whether to include source info fields.


        Returns:
            Tuple of column names based on inclusion criteria.

        Example:
            >>> datagram.keys()  # Data columns only
            ('user_id', 'name', 'email')
            >>> datagram.keys(include_meta_columns=True)
            ('user_id', 'name', 'email', f'{orcapod.META_PREFIX}processed_at', f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_meta_columns=["pipeline"])
            ('user_id', 'name', 'email',f'{orcapod.META_PREFIX}pipeline_version')
            >>> datagram.keys(include_context=True)
            ('user_id', 'name', 'email', f'{orcapod.CONTEXT_KEY}')
        """
        ...

    def types(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> TypeSpec:
        """
        Return type specification mapping field names to Python types.

        The TypeSpec enables type checking and validation throughout the system.

        Args:
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.
            include_meta_columns: Controls meta column type inclusion.
                - False: Exclude meta column types (default)
                - True: Include all meta column types
                - Collection[str]: Include meta column types matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context type.
            include_source: Whether to include source info fields.

        Returns:
            TypeSpec mapping field names to their Python types.

        Example:
            >>> datagram.types()
            {'user_id': <class 'int'>, 'name': <class 'str'>}
        """
        ...

    def arrow_schema(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> "pa.Schema":
        """
        Return PyArrow schema representation.

        The schema provides structured field and type information for efficient
        serialization and deserialization with PyArrow.

        Args:
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.
            include_meta_columns: Controls meta column schema inclusion.
                - False: Exclude meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include context column.
            include_source: Whether to include source info fields.


        Returns:
            PyArrow Schema describing the datagram structure.

        Example:
            >>> schema = datagram.arrow_schema()
            >>> schema.names
            ['user_id', 'name']
        """
        ...

    def as_dict(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert datagram to dictionary format.

        Provides a simple key-value representation useful for debugging,
        serialization, and interop with dict-based APIs.

        Args:
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context key.
            include_source: Whether to include source info fields.


        Returns:
            Dictionary with requested columns as key-value pairs.

        Example:
            >>> data = datagram.as_dict()  # {'user_id': 123, 'name': 'Alice'}
            >>> full_data = datagram.as_dict(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
        """
        ...

    def as_table(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> "pa.Table":
        """
        Convert datagram to PyArrow Table format.

        Provides a standardized columnar representation suitable for analysis,
        processing, and interoperability with Arrow-based tools.

        Args:
            include_all_info: If True, include all available information. This option supersedes all other inclusion options.
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.
            include_context: Whether to include the context column.
            include_source: Whether to include source info columns in the schema.

        Returns:
            PyArrow Table with requested columns.

        Example:
            >>> table = datagram.as_table()  # Data columns only
            >>> full_table = datagram.as_table(
            ...     include_meta_columns=True,
            ...     include_context=True
            ... )
            >>> filtered = datagram.as_table(include_meta_columns=["pipeline"])   # same as passing f"{orcapod.META_PREFIX}pipeline"
        """
        ...

    # TODO: add this back
    # def as_arrow_compatible_dict(
    #     self,
    #     include_all_info: bool = False,
    #     include_meta_columns: bool | Collection[str] = False,
    #     include_context: bool = False,
    #     include_source: bool = False,
    # ) -> dict[str, Any]:
    #     """Extended version with source info support."""
    #     ...

    def as_datagram(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_source: bool = False,
    ) -> Datagram:
        """
        Convert the packet to a Datagram.

        Args:
            include_meta_columns: Controls meta column inclusion.
                - False: Exclude all meta columns (default)
                - True: Include all meta columns
                - Collection[str]: Include meta columns matching these prefixes. If absent,
                    {orcapod.META_PREFIX} ('__') prefix is prepended to each key.

        Returns:
            Datagram: Datagram representation of packet data
        """
        ...

    def source_info(self) -> dict[str, str | None]:
        """
        Return metadata about the packet's source/origin.

        Provides debugging and lineage information about where the packet
        originated. May include information like:
        - File paths for file-based sources
        - Database connection strings
        - API endpoints
        - Processing pipeline information

        Returns:
            dict[str, str | None]: Source information for each data column as key-value pairs.
        """
        ...

    def with_source_info(
        self,
        **source_info: str | None,
    ) -> Self:
        """
        Create new packet with updated source information.

        Adds or updates source metadata for the packet. This is useful for
        tracking data provenance and lineage through the computational graph.

        Args:
            **source_info: Source metadata as keyword arguments.

        Returns:
            New packet instance with updated source information.

        Example:
            >>> updated_packet = packet.with_source_info(
            ...     file_path="/new/path/to/file.txt",
            ...     source_id="source_123"
            ... )
        """
        ...


class PodFunction(Protocol):
    """
    A function suitable for use in a FunctionPod.

    PodFunctions define the computational logic that operates on individual
    packets within a Pod. They represent pure functions that transform
    data values without side effects.

    These functions are designed to be:
    - Stateless: No dependency on external state
    - Deterministic: Same inputs always produce same outputs
    - Serializable: Can be cached and distributed
    - Type-safe: Clear input/output contracts

    PodFunctions accept named arguments corresponding to packet fields
    and return transformed data values.
    """

    def __call__(self, **kwargs: DataValue) -> None | DataValue:
        """
        Execute the pod function with the given arguments.

        The function receives packet data as named arguments and returns
        either transformed data or None (for filtering operations).

        Args:
            **kwargs: Named arguments mapping packet fields to data values

        Returns:
            None: Filter out this packet (don't include in output)
            DataValue: Single transformed value

        Raises:
            TypeError: If required arguments are missing
            ValueError: If argument values are invalid
        """
        ...


class Labelable(Protocol):
    """
    Protocol for objects that can have a human-readable label.

    Labels provide meaningful names for objects in the computational graph,
    making debugging, visualization, and monitoring much easier. They serve
    as human-friendly identifiers that complement the technical identifiers
    used internally.

    Labels are optional but highly recommended for:
    - Debugging complex computational graphs
    - Visualization and monitoring tools
    - Error messages and logging
    - User interfaces and dashboards
    """

    @property
    def label(self) -> str | None:
        """
        Return the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Returns:
            str: Human-readable label for this object
            None: No label is set (will use default naming)
        """
        ...


class Stream(ContentIdentifiable, Labelable, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (Tag, Packet) pairs flowing through the
    computational graph. They are the fundamental data structure connecting
    kernels and carrying both data and metadata.

    Streams can be either:
    - Static: Immutable snapshots created at a specific point in time
    - Live: Dynamic streams that stay current with upstream dependencies

    All streams provide:
    - Iteration over (tag, packet) pairs
    - Type information and schema access
    - Lineage information (source kernel and upstream streams)
    - Basic caching and freshness tracking
    - Conversion to common formats (tables, dictionaries)
    """

    @property
    def source(self) -> "Kernel | None":
        """
        The kernel that produced this stream.

        This provides lineage information for tracking data flow through
        the computational graph. Root streams (like file sources) may
        have no source kernel.

        Returns:
            Kernel: The source kernel that created this stream
            None: This is a root stream with no source kernel
        """
        ...

    @property
    def upstreams(self) -> tuple["Stream", ...]:
        """
        Input streams used to produce this stream.

        These are the streams that were provided as input to the source
        kernel when this stream was created. Used for dependency tracking
        and cache invalidation.

        Returns:
            tuple[Stream, ...]: Upstream dependency streams (empty for sources)
        """
        ...

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Available keys/fields in the stream content.

        Returns the field names present in both tags and packets.
        This provides schema information without requiring type details,
        useful for:
        - Schema inspection and exploration
        - Query planning and optimization
        - Field validation and mapping

        Returns:
            tuple[tuple[str, ...], tuple[str, ...]]: (tag_keys, packet_keys)
        """
        ...

    def types(self) -> tuple[TypeSpec, TypeSpec]:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and packets in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types)
        """
        ...

    @property
    def last_modified(self) -> datetime | None:
        """
        When the stream's content was last modified.

        This property is crucial for caching decisions and dependency tracking:
        - datetime: Content was last modified at this time (cacheable)
        - None: Content is never stable, always recompute (some dynamic streams)

        Both static and live streams typically return datetime values, but
        live streams update this timestamp whenever their content changes.

        Returns:
            datetime: Timestamp of last modification for most streams
            None: Stream content is never stable (some special dynamic streams)
        """
        ...

    @property
    def is_current(self) -> bool:
        """
        Whether the stream is up-to-date with its dependencies.

        A stream is current if its content reflects the latest state of its
        source kernel and upstream streams. This is used for cache validation
        and determining when refresh is needed.

        For live streams, this should always return True since they stay
        current automatically. For static streams, this indicates whether
        the cached content is still valid.

        Returns:
            bool: True if stream is up-to-date, False if refresh needed
        """
        ...

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Iterate over (tag, packet) pairs in the stream.

        This is the primary way to access stream data. The behavior depends
        on the stream type:
        - Static streams: Return cached/precomputed data
        - Live streams: May trigger computation and always reflect current state

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def iter_packets(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Alias for __iter__ for explicit packet iteration.

        Provides a more explicit method name when the intent is to iterate
        over packets specifically, improving code readability.

        This method must return an immutable iterator -- that is, the returned iterator
        should not change and must consistently return identical tag,packet pairs across
        multiple iterations of the iterator.

        Note that this is NOT to mean that multiple invocation of `iter_packets` must always
        return an identical iterator. The iterator returned by `iter_packets` may change
        between invocations, but the iterator itself must not change. Consequently, it should be understood
        that the returned iterators may be a burden on memory if the stream is large or infinite.

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_content_hash: bool | str = False,
    ) -> "pa.Table":
        """
        Convert the entire stream to a PyArrow Table.

        Materializes all (tag, packet) pairs into a single table for
        analysis and processing. This operation may be expensive for
        large streams or live streams that need computation.

        If include_content_hash is True, an additional column called "_content_hash"
        containing the content hash of each packet is included. If include_content_hash
        is a string, it is used as the name of the content hash column.

        Returns:
            pa.Table: Complete stream data as a PyArrow Table
        """
        ...

    def flow(self) -> Collection[tuple[Tag, Packet]]:
        """
        Return the entire stream as a collection of (tag, packet) pairs.

        This method materializes the stream content into a list or similar
        collection type. It is useful for small streams or when you need
        to process all data at once.

        Returns:
            Collection[tuple[Tag, Packet]]: All (tag, packet) pairs in the stream
        """
        ...


class LiveStream(Stream, Protocol):
    """
    A stream that automatically stays up-to-date with its upstream dependencies.

    LiveStream extends the base Stream protocol with capabilities for "up-to-date"
    data flow and reactive computation. Unlike static streams which represent
    snapshots, LiveStreams provide the guarantee that their content always
    reflects the current state of their dependencies.

    Key characteristics:
    - Automatically refresh the stream if changes in the upstreams are detected
    - Track last_modified timestamp when content changes
    - Support manual refresh triggering and invalidation
    - By design, LiveStream would return True for is_current except when auto-update fails.

    LiveStreams are always returned by Kernel.__call__() methods, ensuring
    that normal kernel usage produces live, up-to-date results.

    Caching behavior:
    - last_modified updates whenever content changes
    - Can be cached based on dependency timestamps
    - Invalidation happens automatically when upstreams change

    Use cases:
    - Real-time data processing pipelines
    - Reactive user interfaces
    - Monitoring and alerting systems
    - Dynamic dashboard updates
    - Any scenario requiring current data
    """

    def refresh(self, force: bool = False) -> bool:
        """
        Manually trigger a refresh of this stream's content.

        Forces the stream to check its upstream dependencies and update
        its content if necessary. This is useful when:
        - You want to ensure the latest data before a critical operation
        - You need to force computation at a specific time
        - You're debugging data flow issues
        - You want to pre-compute results for performance
        Args:
            force: If True, always refresh even if the stream is current.
                   If False, only refresh if the stream is not current.

        Returns:
            bool: True if the stream was refreshed, False if it was already current.
        Note: LiveStream refreshes automatically on access, so this
        method may be a no-op for some implementations. However, it's
        always safe to call if you need to control when the cache is refreshed.
        """
        ...

    def invalidate(self) -> None:
        """
        Mark this stream as invalid, forcing a refresh on next access.

        This method is typically called when:
        - Upstream dependencies have changed
        - The source kernel has been modified
        - External data sources have been updated
        - Manual cache invalidation is needed

        The stream will automatically refresh its content the next time
        it's accessed (via iteration, as_table(), etc.).

        This is more efficient than immediate refresh when you know the
        data will be accessed later.
        """
        ...


class Kernel(ContentIdentifiable, Labelable, Protocol):
    """
    The fundamental unit of computation in Orcapod.

    Kernels are the building blocks of computational graphs, transforming
    zero, one, or more input streams into a single output stream. They
    encapsulate computation logic while providing consistent interfaces
    for validation, type checking, and execution.

    Key design principles:
    - Immutable: Kernels don't change after creation
    - Deterministic: Same inputs always produce same outputs
    - Composable: Kernels can be chained and combined
    - Trackable: All invocations are recorded for lineage
    - Type-safe: Strong typing and validation throughout

    Execution modes:
    - __call__(): Full-featured execution with tracking, returns LiveStream
    - forward(): Pure computation without side effects, returns Stream

    The distinction between these modes enables both production use (with
    full tracking) and testing/debugging (without side effects).
    """

    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Return a unique identifier for this Pod.

        The pod_id is used for caching and tracking purposes. It should
        uniquely identify the Pod's computational logic, parameters, and
        any relevant metadata that affects its behavior.

        Returns:
            tuple[str, ...]: Unique identifier for this Pod
        """
        ...

    @property
    def data_context_key(self) -> str:
        """
        Return the context key for this kernel's data processing.

        The context key is used to interpret how data columns should be
        processed and converted. It provides semantic meaning to the data
        being processed by this kernel.

        Returns:
            str: Context key for this kernel's data processing
        """
        ...

    @property
    def last_modified(self) -> datetime | None:
        """
        When the kernel was last modified. For most kernels, this is the timestamp
        of the kernel creation.
        """
        ...

    def __call__(
        self, *streams: Stream, label: str | None = None, **kwargs
    ) -> LiveStream:
        """
        Main interface for kernel invocation with full tracking and guarantees.

        This is the primary way to invoke kernels in production. It provides
        a complete execution pipeline:
        1. Validates input streams against kernel requirements
        2. Registers the invocation with the computational graph
        3. Calls forward() to perform the actual computation
        4. Ensures the result is a LiveStream that stays current

        The returned LiveStream automatically stays up-to-date with its
        upstream dependencies, making it suitable for real-time processing
        and reactive applications.

        Args:
            *streams: Input streams to process (can be empty for source kernels)
            label: Optional label for this invocation (overrides kernel.label)
            **kwargs: Additional arguments for kernel configuration

        Returns:
            LiveStream: Live stream that stays up-to-date with upstreams

        Raises:
            ValidationError: If input streams are invalid for this kernel
            TypeMismatchError: If stream types are incompatible
            ValueError: If required arguments are missing
        """
        ...

    def forward(self, *streams: Stream) -> Stream:
        """
        Perform the actual computation without side effects.

        This method contains the core computation logic and should be
        overridden by subclasses. It performs pure computation without:
        - Registering with the computational graph
        - Performing validation (caller's responsibility)
        - Guaranteeing result type (may return static or live streams)

        The returned stream must be accurate at the time of invocation but
        need not stay up-to-date with upstream changes. This makes forward()
        suitable for:
        - Testing and debugging
        - Batch processing where currency isn't required
        - Internal implementation details

        Args:
            *streams: Input streams to process

        Returns:
            Stream: Result of the computation (may be static or live)
        """
        ...

    def output_types(self, *streams: Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        Determine output types without triggering computation.

        This method performs type inference based on input stream types,
        enabling efficient type checking and stream property queries.
        It should be fast and not trigger any expensive computation.

        Used for:
        - Pre-execution type validation
        - Query planning and optimization
        - Schema inference in complex pipelines
        - IDE support and developer tooling

        Args:
            *streams: Input streams to analyze

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types) for output

        Raises:
            ValidationError: If input types are incompatible
            TypeError: If stream types cannot be processed
        """
        ...

    def validate_inputs(self, *streams: Stream) -> None:
        """
        Validate input streams, raising exceptions if incompatible.

        This method is called automatically by __call__ before computation
        to provide fail-fast behavior. It should check:
        - Number of input streams
        - Stream types and schemas
        - Any kernel-specific requirements
        - Business logic constraints

        The goal is to catch errors early, before expensive computation
        begins, and provide clear error messages for debugging.

        Args:
            *streams: Input streams to validate

        Raises:
            ValidationError: If streams are invalid for this kernel
            TypeError: If stream types are incompatible
            ValueError: If stream content violates business rules
        """
        ...

    def identity_structure(self, streams: Collection[Stream] | None = None) -> Any:
        """
        Generate a unique identity structure for this kernel and/or kernel invocation.
        When invoked without streams, it should return a structure
        that uniquely identifies the kernel itself (e.g., class name, parameters).
        When invoked with streams, it should include the identity of the streams
        to distinguish different invocations of the same kernel.

        This structure is used for:
        - Caching and memoization
        - Debugging and error reporting
        - Tracking kernel invocations in computational graphs

        Args:
            streams: Optional input streams for this invocation. If None, identity_structure is
                based solely on the kernel. If streams are provided, they are included in the identity
                to differentiate between different invocations of the same kernel.

        Returns:
            Any: Unique identity structure (e.g., tuple of class name and stream identities)
        """
        ...


class Pod(Kernel, Protocol):
    """
    Specialized kernel for packet-level processing with advanced caching.

    Pods represent a different computational model from regular kernels:
    - Process data one packet at a time (enabling fine-grained parallelism)
    - Support just-in-time evaluation (computation deferred until needed)
    - Provide stricter type contracts (clear input/output schemas)
    - Enable advanced caching strategies (packet-level caching)

    The Pod abstraction is ideal for:
    - Expensive computations that benefit from caching
    - Operations that can be parallelized at the packet level
    - Transformations with strict type contracts
    - Processing that needs to be deferred until access time
    - Functions that operate on individual data items

    Pods use a different execution model where computation is deferred
    until results are actually needed, enabling efficient resource usage
    and fine-grained caching.
    """

    def input_packet_types(self) -> TypeSpec:
        """
        TypeSpec for input packets that this Pod can process.

        Defines the exact schema that input packets must conform to.
        Pods are typically much stricter about input types than regular
        kernels, requiring precise type matching for their packet-level
        processing functions.

        This specification is used for:
        - Runtime type validation
        - Compile-time type checking
        - Schema inference and documentation
        - Input validation and error reporting

        Returns:
            TypeSpec: Dictionary mapping field names to required packet types
        """
        ...

    def output_packet_types(self) -> TypeSpec:
        """
        TypeSpec for output packets that this Pod produces.

        Defines the schema of packets that will be produced by this Pod.
        This is typically determined by the Pod's computational function
        and is used for:
        - Type checking downstream kernels
        - Schema inference in complex pipelines
        - Query planning and optimization
        - Documentation and developer tooling

        Returns:
            TypeSpec: Dictionary mapping field names to output packet types
        """
        ...

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        """
        Process a single packet with its associated tag.

        This is the core method that defines the Pod's computational behavior.
        It processes one (tag, packet) pair at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Tag transformation (modify metadata)
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            tag: Metadata associated with the packet
            packet: The data payload to process

        Returns:
            tuple[Tag, Packet | None]:
                - Tag: Output tag (may be modified from input)
                - Packet: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...


class Source(Kernel, Stream, Protocol):
    """
    Entry point for data into the computational graph.

    Sources are special objects that serve dual roles:
    - As Kernels: Can be invoked to produce streams
    - As Streams: Directly provide data without upstream dependencies

    Sources represent the roots of computational graphs and typically
    interface with external data sources. They bridge the gap between
    the outside world and the Orcapod computational model.

    Common source types:
    - File readers (CSV, JSON, Parquet, etc.)
    - Database connections and queries
    - API endpoints and web services
    - Generated data sources (synthetic data)
    - Manual data input and user interfaces
    - Message queues and event streams

    Sources have unique properties:
    - No upstream dependencies (upstreams is empty)
    - Can be both invoked and iterated
    - Serve as the starting point for data lineage
    - May have their own refresh/update mechanisms
    """

    pass


class Tracker(Protocol):
    """
    Records kernel invocations and stream creation for computational graph tracking.

    Trackers are responsible for maintaining the computational graph by recording
    relationships between kernels, streams, and invocations. They enable:
    - Lineage tracking and data provenance
    - Caching and memoization strategies
    - Debugging and error analysis
    - Performance monitoring and optimization
    - Reproducibility and auditing

    Multiple trackers can be active simultaneously, each serving different
    purposes (e.g., one for caching, another for debugging, another for
    monitoring). This allows for flexible and composable tracking strategies.

    Trackers can be selectively activated/deactivated to control overhead
    and focus on specific aspects of the computational graph.
    """

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the tracker.

        When active, the tracker will record all kernel invocations and
        stream creations. When inactive, no recording occurs, reducing
        overhead for performance-critical sections.

        Args:
            active: True to activate recording, False to deactivate
        """
        ...

    def is_active(self) -> bool:
        """
        Check if the tracker is currently recording invocations.

        Returns:
            bool: True if tracker is active and recording, False otherwise
        """
        ...

    def record_kernel_invocation(
        self, kernel: Kernel, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a kernel invocation in the computational graph.

        This method is called whenever a kernel is invoked. The tracker
        should record:
        - The kernel and its properties
        - The input streams that were used as input
        - Timing and performance information
        - Any relevant metadata

        Args:
            kernel: The kernel that was invoked
            upstreams: The input streams used for this invocation
        """
        ...

    def record_pod_invocation(
        self, pod: Pod, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a pod invocation in the computational graph.

        This method is called whenever a pod is invoked. The tracker
        should record:
        - The pod and its properties
        - The upstream streams that were used as input
        - Timing and performance information
        - Any relevant metadata

        Args:
            pod: The pod that was invoked
            upstreams: The input streams used for this invocation
        """
        ...


class TrackerManager(Protocol):
    """
    Manages multiple trackers and coordinates their activity.

    The TrackerManager provides a centralized way to:
    - Register and manage multiple trackers
    - Coordinate recording across all active trackers
    - Provide a single interface for graph recording
    - Enable dynamic tracker registration/deregistration

    This design allows for:
    - Multiple concurrent tracking strategies
    - Pluggable tracking implementations
    - Easy testing and debugging (mock trackers)
    - Performance optimization (selective tracking)
    """

    def get_active_trackers(self) -> list[Tracker]:
        """
        Get all currently active trackers.

        Returns only trackers that are both registered and active,
        providing the list of trackers that will receive recording events.

        Returns:
            list[Tracker]: List of trackers that are currently recording
        """
        ...

    def register_tracker(self, tracker: Tracker) -> None:
        """
        Register a new tracker in the system.

        The tracker will be included in future recording operations
        if it is active. Registration is separate from activation
        to allow for dynamic control of tracking overhead.

        Args:
            tracker: The tracker to register
        """
        ...

    def deregister_tracker(self, tracker: Tracker) -> None:
        """
        Remove a tracker from the system.

        The tracker will no longer receive recording notifications
        even if it is still active. This is useful for:
        - Cleaning up temporary trackers
        - Removing failed or problematic trackers
        - Dynamic tracker management

        Args:
            tracker: The tracker to remove
        """
        ...

    def record_kernel_invocation(
        self, kernel: Kernel, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a stream in all active trackers.

        This method broadcasts the stream recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            stream: The stream to record in all active trackers
        """
        ...

    def record_pod_invocation(
        self, pod: Pod, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a stream in all active trackers.

        This method broadcasts the stream recording to all currently`
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            stream: The stream to record in all active trackers
        """
        ...

    def no_tracking(self) -> ContextManager[None]: ...
