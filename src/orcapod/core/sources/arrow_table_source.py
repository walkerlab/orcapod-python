from collections.abc import Collection
from re import S
from typing import TYPE_CHECKING, Any


from orcapod.core.streams import TableStream
from orcapod.protocols import core_protocols as dp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.system_constants import constants
from orcapod.core import arrow_data_utils
from orcapod.core.sources.source_registry import GLOBAL_SOURCE_REGISTRY, SourceRegistry

from orcapod.core.sources.base import SourceBase

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class ArrowTableSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    SOURCE_ID = "arrow"

    def __init__(
        self,
        arrow_table: "pa.Table",
        tag_columns: Collection[str] = (),
        source_name: str | None = None,
        source_registry: SourceRegistry | None = None,
        auto_register: bool = True,
        preserve_system_columns: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # clean the table, dropping any system columns
        # TODO: consider special treatment of system columns if provided
        if not preserve_system_columns:
            arrow_table = arrow_data_utils.drop_system_columns(arrow_table)

        self.tag_columns = [
            col for col in tag_columns if col in arrow_table.column_names
        ]

        self.table_hash = self.data_context.arrow_hasher.hash_table(arrow_table)

        if source_name is None:
            source_name = self.content_hash().to_hex()

        self._source_name = source_name

        row_index = list(range(arrow_table.num_rows))

        source_info = [f"{self.source_id}::row_{i}" for i in row_index]

        # add source info
        arrow_table = arrow_data_utils.add_source_info(
            arrow_table, source_info, exclude_columns=tag_columns
        )

        arrow_table = arrow_table.add_column(
            0,
            f"{constants.SYSTEM_TAG_PREFIX}{self.source_id}::row_index",
            pa.array(row_index, pa.int64()),
        )

        self._table = arrow_table

        self._table_stream = TableStream(
            table=self._table,
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

        # Auto-register with global registry
        if auto_register:
            registry = source_registry or GLOBAL_SOURCE_REGISTRY
            registry.register(self.source_id, self)

    @property
    def reference(self) -> tuple[str, ...]:
        return ("arrow_table", self._source_name)

    @property
    def table(self) -> "pa.Table":
        return self._table

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.tag_columns, self.table_hash)

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
