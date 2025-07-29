from abc import abstractmethod

from pyarrow.lib import Table
from orcapod.data.kernels import TrackedKernelBase
from orcapod.protocols import data_protocols as dp
from collections.abc import Collection, Iterator
from orcapod.data.streams import ImmutableTableStream
from typing import TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from orcapod.types import TypeSpec
from datetime import datetime
from orcapod.types import schemas
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from pathlib import Path


if TYPE_CHECKING:
    import polars as pl
    import pandas as pd
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")


class SourceBase(TrackedKernelBase):
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

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_content_hash: bool | str = False,
    ) -> "pa.Table":
        return self().as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_content_hash=include_content_hash,
        )

    def flow(self) -> Collection[tuple[dp.Tag, dp.Packet]]:
        return self().flow()

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all records associated with the node.
        If include_system_columns is True, system columns will be included in the result.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    @property
    def lazy(self) -> "pl.LazyFrame | None":
        records = self.get_all_records(include_system_columns=False)
        if records is not None:
            return pl.LazyFrame(records)
        return None

    @property
    def df(self) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        lazy_df = self.lazy
        if lazy_df is not None:
            return lazy_df.collect()
        return None

    @property
    def polars_df(self) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        return self.df

    @property
    def pandas_df(self) -> "pd.DataFrame | None":
        """
        Return the pandas DataFrame representation of the pod's records.
        """
        records = self.get_all_records(include_system_columns=False)
        if records is not None:
            pandas_df = records.to_pandas()
            pandas_df.set_index(list(self.tag_keys), inplace=True)
            return pandas_df
        return None

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        tag_types, packet_types = self.output_types()
        return tuple(tag_types.keys()), tuple(packet_types.keys())

    def types(self) -> tuple[TypeSpec, TypeSpec]:
        return self.output_types()

    def __iter__(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        return self().__iter__()

    def iter_packets(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        return self().iter_packets()

    # properties and methods to act as a dp.Stream
    @property
    def source(self) -> dp.Kernel | None:
        return self

    @property
    def upstreams(self) -> tuple[dp.Stream, ...]:
        return ()

    def validate_inputs(self, *processed_streams: dp.Stream) -> None:
        pass


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
        self.refresh()

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

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (self.__class__.__name__, str(self.table_path))

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

    def refresh(self) -> None:
        """
        Refresh the delta table to ensure it is up-to-date.
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
