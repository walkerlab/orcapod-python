from abc import abstractmethod
from collections.abc import Collection, Iterator
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from pyarrow.lib import Table

from orcapod.data.kernels import TrackedKernelBase
from orcapod.data.streams import ImmutableTableStream, KernelStream
from orcapod.protocols import data_protocols as dp
from orcapod.types import TypeSpec, schemas
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")


class SourceBase(TrackedKernelBase):
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

    @abstractmethod
    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Pure computation: return a static snapshot of the data.

        This is the core method that subclasses must implement.
        Each call should return a fresh stream representing the current state of the data.
        This is what KernelStream calls when it needs to refresh its data.
        """
        ...

    @abstractmethod
    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """Return the tag and packet types this source produces."""
        ...

    @abstractmethod
    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> dp.Any: ...

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

    def types(self) -> tuple[TypeSpec, TypeSpec]:
        """Delegate to the cached KernelStream."""
        return self().types()

    @property
    def last_modified(self):
        """Delegate to the cached KernelStream."""
        return self().last_modified

    @property
    def is_current(self) -> bool:
        """Delegate to the cached KernelStream."""
        return self().is_current

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

    def flow(self) -> Collection[tuple[dp.Tag, dp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().flow()

    # ==================== LiveStream Protocol (Delegation) ====================

    def refresh(self, force: bool = False) -> bool:
        """Delegate to the cached KernelStream."""
        return self().refresh(force=force)

    def invalidate(self) -> None:
        """Delegate to the cached KernelStream."""
        return self().invalidate()

    # ==================== Source-Specific Utilities ====================

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

        from orcapod.data.streams import ImmutableTableStream

        # Load current state of the file
        table = csv.read_csv(self.file_path)

        return ImmutableTableStream(
            table=table,
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """Infer types from the file (could be cached)."""
        # For demonstration - in practice you might cache this
        sample_stream = self.forward()
        return sample_stream.types()


class ManualDeltaTableSource(SourceBase):
    """
    A source that allows manual delta updates to a table.
    This is useful for testing and debugging purposes.
    """

    def __init__(
        self,
        table_path: str | Path,
        schema: TypeSpec | None = None,
        tag_columns: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the ManualDeltaTableSource with a label and optional data context.
        """
        super().__init__(**kwargs)

        self.table_path = table_path
        self._delta_table: DeltaTable | None = None
        self.load_delta_table()

        if self._delta_table is None:
            if schema is None:
                raise ValueError(
                    "Delta table not found and no schema provided. "
                    "Please provide a valid Delta table path or a schema to create a new table."
                )
            if tag_columns is None:
                raise ValueError(
                    "At least one tag column must be provided when creating a new Delta table."
                )
            python_schema = schemas.PythonSchema(schema)
            arrow_schema = python_schema.to_arrow_schema(
                self.data_context.semantic_type_registry
            )
            fields = []
            for field in arrow_schema:
                if field.name in tag_columns:
                    field = field.with_metadata({b"table": b"True"})
                fields.append(field)
            arrow_schema = pa.schema(fields)

        else:
            arrow_schema = pa.schema(self._delta_table.schema().to_arrow())  # type: ignore
            python_schema = schemas.PythonSchema.from_arrow_schema(
                arrow_schema, self.data_context.semantic_type_registry
            )
            inferred_tag_columns = []
            for field in arrow_schema:
                if (
                    field.metadata is not None
                    and field.metadata.get(b"table", b"False").decode().lower()
                    == "true"
                ):
                    inferred_tag_columns.append(field.name)
            tag_columns = tag_columns or inferred_tag_columns
        self.python_schema = python_schema
        self.arrow_schema = arrow_schema
        self.tag_columns = tag_columns

        self._is_current = True

    @property
    def delta_table_version(self) -> int | None:
        """
        Return the version of the delta table.
        If the table does not exist, return None.
        """
        if self._delta_table is not None:
            return self._delta_table.version()
        return None

    @property
    def is_current(self) -> bool:
        return self._is_current

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        if len(streams) > 0:
            raise ValueError("ManualDeltaTableSource takes no input streams")
        if self._delta_table is None:
            arrow_data = pa.Table.from_pylist([], schema=self.arrow_schema)
        else:
            arrow_data = self._delta_table.to_pyarrow_dataset(
                as_large_types=True
            ).to_table()
        return ImmutableTableStream(arrow_data, self.tag_columns, source=self)

    def kernel_identity_structure(self, streams: Collection[dp.Stream] | None = None):
        """
        Return the identity structure of the kernel.
        This is a unique identifier for the kernel based on its class name and table path.
        """
        return (self.__class__.__name__, str(self.table_path))

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        tag_types: TypeSpec = {}
        packet_types: TypeSpec = {}
        for field, field_type in self.python_schema.items():
            if field in self.tag_columns:
                tag_types[field] = field_type
            else:
                packet_types[field] = field_type
        return tag_types, packet_types

    def get_all_records(self, include_system_columns: bool = False) -> Table | None:
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

    def insert(
        self,
        data: "dict | pa.Table | pl.DataFrame | pd.DataFrame",
    ) -> None:
        """
        Insert data into the delta table.
        """
        if isinstance(data, dict):
            data = pa.Table.from_pylist([data], schema=self.arrow_schema)
        elif isinstance(data, pl.DataFrame):
            data = data.to_arrow()
        elif isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data, schema=self.arrow_schema)

        self._set_modified_time()
        write_deltalake(
            self.table_path,
            data,
            mode="append",
        )

        # update the delta table
        self._delta_table = DeltaTable(self.table_path)

    def load_delta_table(self) -> None:
        """
        Try loading the delta table from the file system.
        """
        current_version = self.delta_table_version
        try:
            delta_table = DeltaTable(self.table_path)
        except TableNotFoundError:
            delta_table = None
        new_version = self.delta_table_version
        if (current_version is None and new_version is not None) or (
            current_version is not None
            and new_version is not None
            and current_version < new_version
        ):
            # delta table has been updated
            self._set_modified_time()
        self._delta_table = delta_table


class GlobSource(SourceBase):
    """
    A source that reads files from the file system using a glob pattern.
    It generates its own stream from the file system.
    """

    def __init__(
        self,
        name: str,
        file_path: str | Path,
        glob_pattern: str,
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self.file_path = Path(file_path)
        self.glob_pattern = glob_pattern

    @staticmethod
    def default_tag_function(file_path: Path) -> dict:
        return {"file_path": str(file_path)}

    def kernel_identity_structure(self, streams: Collection[dp.Stream] | None = None):
        hash_function_kwargs = {
            "include_declaration": True,
            "include_source": True,
            "include_content_hash": True,
            "include_data_context": True,
        }
