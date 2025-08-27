from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any


from orcapod.data.streams import TableStream
from orcapod.protocols import data_protocols as dp
from orcapod.types import DataValue
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.data.system_constants import constants
from orcapod.semantic_types import infer_python_schema_from_pylist_data

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

from orcapod.data.sources.base import SourceBase


class ArrowTableSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_info: dict[str, str | None] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.tag_columns = tag_columns
        self.system_tag_columns = system_tag_columns
        self.source_info = source_info
        self.table_hash = self.data_context.arrow_hasher.hash_table(self.table)
        self._table_stream = TableStream(
            table=self.table,
            tag_columns=self.tag_columns,
            system_tag_columns=self.system_tag_columns,
            source=self,
            upstreams=(),
        )

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.table_hash)

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
    ) -> tuple[dict[str, type], dict[str, type]]:
        """Return tag and packet types based on provided typespecs."""
        return self._table_stream.types(include_system_tags=include_system_tags)
