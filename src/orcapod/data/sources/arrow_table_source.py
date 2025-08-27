from collections.abc import Collection
from typing import TYPE_CHECKING, Any


from orcapod.data.streams import TableStream
from orcapod.protocols import data_protocols as dp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from orcapod.data.system_constants import constants
from orcapod.data import arrow_data_utils

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

from orcapod.data.sources.base import SourceBase


class ArrowTableSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    SOURCE_ID = "arrow"

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        **kwargs,
    ):
        super().__init__(**kwargs)

        # clean the table, dropping any system columns
        # TODO: consider special treatment of system columns if provided
        table = arrow_data_utils.drop_system_columns(table)
        self.table_hash = self.data_context.arrow_hasher.hash_table(table)

        self.tag_columns = [col for col in tag_columns if col in table.column_names]

        # add system tag column, indexing into the array
        system_tag_column = pa.array(list(range(table.num_rows)), pa.int64())

        table = table.add_column(
            0, f"{constants.SYSTEM_TAG_PREFIX}{self.source_info}", system_tag_column
        )

        # add source info
        self._table = arrow_data_utils.add_source_info(
            table, self.source_info, exclude_columns=tag_columns
        )

        self._table_stream = TableStream(
            table=self._table,
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

    @property
    def reference(self) -> tuple[str, ...]:
        return (self.SOURCE_ID, self.table_hash.to_hex())

    @property
    def table(self) -> "pa.Table":
        return self._table

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.source_info, self.table_hash)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        return self().as_table(include_source=include_system_columns)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        return self._table_stream

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """Return tag and packet types based on provided typespecs."""
        return self._table_stream.types(include_system_tags=include_system_tags)
