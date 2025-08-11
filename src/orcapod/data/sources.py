from abc import abstractmethod
from collections.abc import Collection, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from pyarrow.lib import Table

from orcapod.data.kernels import TrackedKernelBase
from orcapod.data.streams import (
    TableStream,
    KernelStream,
    OperatorStreamBaseMixin,
)
from orcapod.errors import DuplicateTagError
from orcapod.protocols import data_protocols as dp
from orcapod.types import DataValue, TypeSpec, typespec_utils
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.data.system_constants import constants
from orcapod.semantic_types import infer_schema_from_pylist_data

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")


class SourceBase(TrackedKernelBase, OperatorStreamBaseMixin):
    """
    Base class for sources that act as both Kernels and LiveStreams.

    Design Philosophy:
    1. Source is fundamentally a Kernel (data loader)
    2. forward() returns static snapshots as a stream (pure computation)
    3. __call__() returns a cached KernelStream (live, tracked)
    4. All stream methods delegate to the cached KernelStream

    This ensures that direct source iteration and source() iteration
    are identical and both benefit from KernelStream's lifecycle management.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Cache the KernelStream for reuse across all stream method calls
        self._cached_kernel_stream: KernelStream | None = None

    # =========================== Kernel Methods ===========================

    # The following are inherited from TrackedKernelBase as abstract methods.
    # @abstractmethod
    # def forward(self, *streams: dp.Stream) -> dp.Stream:
    #     """
    #     Pure computation: return a static snapshot of the data.

    #     This is the core method that subclasses must implement.
    #     Each call should return a fresh stream representing the current state of the data.
    #     This is what KernelStream calls when it needs to refresh its data.
    #     """
    #     ...

    # @abstractmethod
    # def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
    #     """Return the tag and packet types this source produces."""
    #     ...

    # @abstractmethod
    # def kernel_identity_structure(
    #     self, streams: Collection[dp.Stream] | None = None
    # ) -> dp.Any: ...

    def validate_inputs(self, *streams: dp.Stream) -> None:
        """Sources take no input streams."""
        if len(streams) > 0:
            raise ValueError(
                f"{self.__class__.__name__} is a source and takes no input streams"
            )

    def prepare_output_stream(
        self, *streams: dp.Stream, label: str | None = None
    ) -> KernelStream:
        if self._cached_kernel_stream is None:
            self._cached_kernel_stream = super().prepare_output_stream(
                *streams, label=label
            )
        return self._cached_kernel_stream

    def track_invocation(self, *streams: dp.Stream, label: str | None = None) -> None:
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_source_invocation(self, label=label)

    # ==================== Stream Protocol (Delegation) ====================

    @property
    def source(self) -> dp.Kernel | None:
        """Sources are their own source."""
        return self

    @property
    def upstreams(self) -> tuple[dp.Stream, ...]:
        """Sources have no upstream dependencies."""
        return ()

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Delegate to the cached KernelStream."""
        return self().keys()

    def types(self, include_system_tags: bool = False) -> tuple[TypeSpec, TypeSpec]:
        """Delegate to the cached KernelStream."""
        return self().types(include_system_tags=include_system_tags)

    @property
    def last_modified(self):
        """Delegate to the cached KernelStream."""
        return self().last_modified

    @property
    def is_current(self) -> bool:
        """Delegate to the cached KernelStream."""
        return self().is_current

    def __iter__(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        """
        Iterate over the cached KernelStream.

        This allows direct iteration over the source as if it were a stream.
        """
        return self().iter_packets()

    def iter_packets(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().iter_packets()

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_content_hash: bool | str = False,
    ) -> "pa.Table":
        """Delegate to the cached KernelStream."""
        return self().as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_content_hash=include_content_hash,
        )

    def flow(
        self, execution_engine: dp.ExecutionEngine | None = None
    ) -> Collection[tuple[dp.Tag, dp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().flow(execution_engine=execution_engine)

    def run(self, execution_engine: dp.ExecutionEngine | None = None) -> None:
        """
        Run the source node, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        self().run(execution_engine=execution_engine)

    async def run_async(
        self, execution_engine: dp.ExecutionEngine | None = None
    ) -> None:
        """
        Run the source node asynchronously, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        await self().run_async(execution_engine=execution_engine)

    # ==================== LiveStream Protocol (Delegation) ====================

    def refresh(self, force: bool = False) -> bool:
        """Delegate to the cached KernelStream."""
        return self().refresh(force=force)

    def invalidate(self) -> None:
        """Delegate to the cached KernelStream."""
        return self().invalidate()

    # ==================== Source Protocol ====================

    @property
    def tag_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the tag in the pipeline run records.
        This is used to store the run-associated tag info.
        """
        tag_keys, _ = self.keys()
        return tag_keys

    @property
    def packet_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the packet in the pipeline run records.
        This is used to store the run-associated packet info.
        """
        # TODO: consider caching this
        _, packet_keys = self.keys()
        return packet_keys

    @abstractmethod
    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all records from the source.

        This method should be implemented by subclasses to return the full dataset.
        If the source has no records, return None.
        """
        ...

    def as_lazy_frame(self, sort_by_tags: bool = False) -> "pl.LazyFrame | None":
        records = self.get_all_records(include_system_columns=False)
        if records is not None:
            result = pl.LazyFrame(records)
            if sort_by_tags:
                result = result.sort(self.tag_keys)
            return result
        return None

    def as_df(self, sort_by_tags: bool = True) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        lazy_df = self.as_lazy_frame(sort_by_tags=sort_by_tags)
        if lazy_df is not None:
            return lazy_df.collect()
        return None

    def as_polars_df(self, sort_by_tags: bool = True) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        return self.as_df(sort_by_tags=sort_by_tags)

    def as_pandas_df(self, sort_by_tags: bool = True) -> "pd.DataFrame | None":
        """
        Return the pandas DataFrame representation of the pod's records.
        """
        df = self.as_polars_df(sort_by_tags=sort_by_tags)
        if df is not None:
            return df.to_pandas()
        return None

    def reset_cache(self) -> None:
        """
        Clear the cached KernelStream, forcing a fresh one on next access.

        Useful when the underlying data source has fundamentally changed
        (e.g., file path changed, database connection reset).
        """
        if self._cached_kernel_stream is not None:
            self._cached_kernel_stream.invalidate()
        self._cached_kernel_stream = None


# ==================== Example Implementation ====================


class CSVSource(SourceBase):
    """Loads data from a CSV file."""

    def __init__(self, file_path: str, tag_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.tag_columns = tag_columns or []

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        return (self.__class__.__name__, self.file_path, tuple(self.tag_columns))

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        import pyarrow.csv as csv

        from orcapod.data.streams import TableStream

        # Load current state of the file
        table = csv.read_csv(self.file_path)

        return TableStream(
            table=table,
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

    def kernel_output_types(
        self, *streams: dp.Stream, include_system_tags: bool = False
    ) -> tuple[TypeSpec, TypeSpec]:
        """Infer types from the file (could be cached)."""
        # For demonstration - in practice you might cache this
        sample_stream = self.forward()
        return sample_stream.types(include_system_tags=include_system_tags)


class ManualDeltaTableSource(SourceBase):
    """
    A source that allows manual delta updates to a table.
    This is useful for testing and debugging purposes.

    Supports duplicate tag handling:
    - skip_duplicates=True: Use merge operation to only insert new tag combinations
    - skip_duplicates=False: Raise error if duplicate tags would be created
    """

    def __init__(
        self,
        table_path: str | Path,
        python_schema: dict[str, type] | None = None,
        tag_columns: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the ManualDeltaTableSource with a label and optional data context.
        """
        super().__init__(**kwargs)

        self.table_path = Path(table_path)
        self._delta_table: DeltaTable | None = None
        self.load_delta_table()

        if self._delta_table is None:
            if python_schema is None:
                raise ValueError(
                    "Delta table not found and no schema provided. "
                    "Please provide a valid Delta table path or a schema to create a new table."
                )
            if tag_columns is None:
                raise ValueError(
                    "At least one tag column must be provided when creating a new Delta table."
                )
            arrow_schema = (
                self._data_context.type_converter.python_schema_to_arrow_schema(
                    python_schema
                )
            )

            fields = []
            for field in arrow_schema:
                if field.name in tag_columns:
                    field = field.with_metadata({b"tag": b"True"})
                fields.append(field)
            arrow_schema = pa.schema(fields)

        else:
            arrow_schema = pa.schema(self._delta_table.schema().to_arrow())
            python_schema = (
                self._data_context.type_converter.arrow_schema_to_python_schema(
                    arrow_schema
                )
            )

            inferred_tag_columns = []
            for field in arrow_schema:
                if (
                    field.metadata is not None
                    and field.metadata.get(b"tag", b"False").decode().lower() == "true"
                ):
                    inferred_tag_columns.append(field.name)
            tag_columns = tag_columns or inferred_tag_columns

        self.python_schema = python_schema
        self.arrow_schema = arrow_schema
        self.tag_columns = list(tag_columns) if tag_columns else []

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (self.__class__.__name__, str(self.table_path))

    @property
    def delta_table_version(self) -> int | None:
        """
        Return the version of the delta table.
        If the table does not exist, return None.
        """
        if self._delta_table is not None:
            return self._delta_table.version()
        return None

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """Load current delta table data as a stream."""
        if len(streams) > 0:
            raise ValueError("ManualDeltaTableSource takes no input streams")

        if self._delta_table is None:
            arrow_data = pa.Table.from_pylist([], schema=self.arrow_schema)
        else:
            arrow_data = self._delta_table.to_pyarrow_dataset(
                as_large_types=True
            ).to_table()

        return TableStream(
            arrow_data, tag_columns=self.tag_columns, source=self, upstreams=()
        )

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        """
        Return the identity structure of the kernel.
        This is a unique identifier for the kernel based on its class name and table path.
        """
        return (self.__class__.__name__, str(self.table_path))

    def kernel_output_types(
        self, *streams: dp.Stream, include_system_tags: bool = False
    ) -> tuple[TypeSpec, TypeSpec]:
        """Return tag and packet types based on schema and tag columns."""
        # TODO: auto add system entry tag
        tag_types: TypeSpec = {}
        packet_types: TypeSpec = {}
        for field, field_type in self.python_schema.items():
            if field in self.tag_columns:
                tag_types[field] = field_type
            else:
                packet_types[field] = field_type
        return tag_types, packet_types

    def get_all_records(self, include_system_columns: bool = False) -> pa.Table | None:
        """Get all records from the delta table."""
        if self._delta_table is None:
            return None

        arrow_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()

        if not include_system_columns:
            arrow_data = arrow_data.drop(
                [col for col in arrow_data.column_names if col.startswith("_")]
            )
        return arrow_data

    def _normalize_data_to_table(
        self, data: "dict | pa.Table | pl.DataFrame | pd.DataFrame"
    ) -> pa.Table:
        """Convert input data to PyArrow Table with correct schema."""
        if isinstance(data, dict):
            return pa.Table.from_pylist([data], schema=self.arrow_schema)
        elif isinstance(data, pa.Table):
            return data
        else:
            # Handle polars/pandas DataFrames
            if hasattr(data, "to_arrow"):  # Polars DataFrame
                return data.to_arrow()  # type: ignore
            elif hasattr(data, "to_pandas"):  # Polars to pandas fallback
                return pa.Table.from_pandas(data.to_pandas(), schema=self.arrow_schema)  # type: ignore
            else:  # Assume pandas DataFrame
                return pa.Table.from_pandas(
                    cast(pd.DataFrame, data), schema=self.arrow_schema
                )

    def _check_for_duplicates(self, new_data: pa.Table) -> None:
        """
        Check if new data contains tag combinations that already exist.
        Raises DuplicateTagError if duplicates found.
        """
        if self._delta_table is None or not self.tag_columns:
            return  # No existing data or no tag columns to check

        # Get existing tag combinations
        existing_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()
        if len(existing_data) == 0:
            return  # No existing data

        # Extract tag combinations from existing data
        existing_tags = existing_data.select(self.tag_columns)
        new_tags = new_data.select(self.tag_columns)

        # Convert to sets of tuples for comparison
        existing_tag_tuples = set()
        for i in range(len(existing_tags)):
            tag_tuple = tuple(
                existing_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            existing_tag_tuples.add(tag_tuple)

        # Check for duplicates in new data
        duplicate_tags = []
        for i in range(len(new_tags)):
            tag_tuple = tuple(
                new_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            if tag_tuple in existing_tag_tuples:
                duplicate_tags.append(tag_tuple)

        if duplicate_tags:
            tag_names = ", ".join(self.tag_columns)
            duplicate_strs = [str(tags) for tags in duplicate_tags]
            raise DuplicateTagError(
                f"Duplicate tag combinations found for columns [{tag_names}]: "
                f"{duplicate_strs}. Use skip_duplicates=True to merge instead."
            )

    def _merge_data(self, new_data: pa.Table) -> None:
        """
        Merge new data using Delta Lake merge operation.
        Only inserts rows where tag combinations don't already exist.
        """
        if self._delta_table is None:
            # No existing table, just write the data
            write_deltalake(
                self.table_path,
                new_data,
                mode="overwrite",
            )
        else:
            # Use merge operation - only insert if tag combination doesn't exist
            # Build merge condition based on tag columns
            # Format: "target.col1 = source.col1 AND target.col2 = source.col2"
            merge_conditions = " AND ".join(
                f"target.{col} = source.{col}" for col in self.tag_columns
            )

            try:
                # Use Delta Lake's merge functionality
                (
                    self._delta_table.merge(
                        source=new_data,
                        predicate=merge_conditions,
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_not_matched_insert_all()  # Insert when no match found
                    .execute()
                )
            except Exception:
                # Fallback: manual duplicate filtering if merge fails
                self._manual_merge_fallback(new_data)

    def _manual_merge_fallback(self, new_data: pa.Table) -> None:
        """
        Fallback merge implementation that manually filters duplicates.
        """
        if self._delta_table is None or not self.tag_columns:
            write_deltalake(self.table_path, new_data, mode="append")
            return

        # Get existing tag combinations
        existing_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()
        existing_tags = existing_data.select(self.tag_columns)

        # Create set of existing tag tuples
        existing_tag_tuples = set()
        for i in range(len(existing_tags)):
            tag_tuple = tuple(
                existing_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            existing_tag_tuples.add(tag_tuple)

        # Filter new data to only include non-duplicate rows
        filtered_rows = []
        new_tags = new_data.select(self.tag_columns)

        for i in range(len(new_data)):
            tag_tuple = tuple(
                new_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            if tag_tuple not in existing_tag_tuples:
                # Extract this row
                row_dict = {}
                for col_name in new_data.column_names:
                    row_dict[col_name] = new_data.column(col_name)[i].as_py()
                filtered_rows.append(row_dict)

        # Only append if there are new rows to add
        if filtered_rows:
            filtered_table = pa.Table.from_pylist(
                filtered_rows, schema=self.arrow_schema
            )
            write_deltalake(self.table_path, filtered_table, mode="append")

    def insert(
        self,
        data: "dict | pa.Table | pl.DataFrame | pd.DataFrame",
        skip_duplicates: bool = False,
    ) -> None:
        """
        Insert data into the delta table.

        Args:
            data: Data to insert (dict, PyArrow Table, Polars DataFrame, or Pandas DataFrame)
            skip_duplicates: If True, use merge operation to skip duplicate tag combinations.
                           If False, raise error if duplicate tag combinations are found.

        Raises:
            DuplicateTagError: If skip_duplicates=False and duplicate tag combinations are found.
        """
        # Normalize data to PyArrow Table
        new_data_table = self._normalize_data_to_table(data)

        if skip_duplicates:
            # Use merge operation to only insert new tag combinations
            self._merge_data(new_data_table)
        else:
            # Check for duplicates first, raise error if found
            self._check_for_duplicates(new_data_table)

            # No duplicates found, safe to append
            write_deltalake(self.table_path, new_data_table, mode="append")

        # Update our delta table reference and mark as modified
        self._set_modified_time()
        self._delta_table = DeltaTable(self.table_path)

        # Invalidate any cached streams
        self.invalidate()

    def load_delta_table(self) -> None:
        """
        Try loading the delta table from the file system.
        """
        current_version = self.delta_table_version
        try:
            delta_table = DeltaTable(self.table_path)
        except TableNotFoundError:
            delta_table = None

        if delta_table is not None:
            new_version = delta_table.version()
            if (current_version is None) or (
                current_version is not None and new_version > current_version
            ):
                # Delta table has been updated
                self._set_modified_time()

        self._delta_table = delta_table


class DictSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    def __init__(
        self,
        tags: Collection[dict[str, DataValue]],
        packets: Collection[dict[str, DataValue]],
        tag_typespec: dict[str, type] | None = None,
        packet_typespec: dict[str, type] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.tags = list(tags)
        self.packets = list(packets)
        if len(self.tags) != len(self.packets) or len(self.tags) == 0:
            raise ValueError(
                "Tags and packets must be non-empty collections of equal length"
            )
        self.tag_typespec = tag_typespec or infer_schema_from_pylist_data(self.tags)
        self.packet_typespec = packet_typespec or infer_schema_from_pylist_data(
            self.packets
        )
        source_info = ":".join(self.kernel_id)
        self.source_info = {
            f"{constants.SOURCE_PREFIX}{k}": f"{source_info}:{k}"
            for k in self.tag_typespec
        }

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        return (
            self.__class__.__name__,
            tuple(self.tag_typespec.items()),
            tuple(self.packet_typespec.items()),
        )

    def get_all_records(self, include_system_columns: bool = False) -> Table | None:
        return self().as_table(include_source=include_system_columns)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        tag_arrow_schema = (
            self._data_context.type_converter.python_schema_to_arrow_schema(
                self.tag_typespec
            )
        )
        packet_arrow_schema = (
            self._data_context.type_converter.python_schema_to_arrow_schema(
                self.packet_typespec
            )
        )

        joined_data = [
            {**tag, **packet} for tag, packet in zip(self.tags, self.packets)
        ]

        table = pa.Table.from_pylist(
            joined_data,
            schema=arrow_utils.join_arrow_schemas(
                tag_arrow_schema, packet_arrow_schema
            ),
        )

        return TableStream(
            table=table,
            tag_columns=self.tag_keys,
            source=self,
            upstreams=(),
        )

    def kernel_output_types(
        self, *streams: dp.Stream, include_system_tags: bool = False
    ) -> tuple[TypeSpec, TypeSpec]:
        """Return tag and packet types based on provided typespecs."""
        # TODO: add system tag
        return self.tag_typespec, self.packet_typespec
