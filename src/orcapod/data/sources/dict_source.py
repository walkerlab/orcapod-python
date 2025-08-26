from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any


from pyarrow.lib import Table

from orcapod.data.streams import TableStream
from orcapod.protocols import data_protocols as dp
from orcapod.types import DataValue
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.data.system_constants import constants
from orcapod.semantic_types import infer_schema_from_pylist_data

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

from orcapod.data.sources.base import SourceBase


class DictSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    def __init__(
        self,
        tags: Collection[dict[str, DataValue]],
        packets: Collection[dict[str, DataValue]],
        tag_schema: Mapping[str, type] | None = None,
        packet_schema: Mapping[str, type] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.tags = list(tags)
        self.packets = list(packets)
        if len(self.tags) != len(self.packets) or len(self.tags) == 0:
            raise ValueError(
                "Tags and packets must be non-empty collections of equal length"
            )
        self.tag_schema = (
            dict(tag_schema) if tag_schema else infer_schema_from_pylist_data(self.tags)
        )
        self.packet_schema = (
            dict(packet_schema)
            if packet_schema
            else infer_schema_from_pylist_data(self.packets)
        )
        source_info = ":".join(self.kernel_id)
        self.source_info = {
            f"{constants.SOURCE_PREFIX}{k}": f"{source_info}:{k}"
            for k in self.tag_schema
        }

    def source_identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            tuple(self.tag_schema.items()),
            tuple(self.packet_schema.items()),
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
                self.tag_schema
            )
        )
        packet_arrow_schema = (
            self._data_context.type_converter.python_schema_to_arrow_schema(
                self.packet_schema
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

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[dict[str, type], dict[str, type]]:
        """Return tag and packet types based on provided typespecs."""
        # TODO: add system tag
        return self.tag_schema, self.packet_schema
