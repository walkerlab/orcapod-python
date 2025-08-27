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


def add_source_field(
    record: dict[str, DataValue], source_info: str
) -> dict[str, DataValue]:
    """Add source information to a record."""
    # for all "regular" fields, add source info
    for key in record.keys():
        if not key.startswith(constants.META_PREFIX) and not key.startswith(
            constants.DATAGRAM_PREFIX
        ):
            record[f"{constants.SOURCE_PREFIX}{key}"] = f"{source_info}:{key}"
    return record


def split_fields_with_prefixes(
    record, prefixes: Collection[str]
) -> tuple[dict[str, DataValue], dict[str, DataValue]]:
    """Split fields in a record into two dictionaries based on prefixes."""
    matching = {}
    non_matching = {}
    for key, value in record.items():
        if any(key.startswith(prefix) for prefix in prefixes):
            matching[key] = value
        else:
            non_matching[key] = value
    return matching, non_matching


def split_system_columns(
    data: list[dict[str, DataValue]],
) -> tuple[list[dict[str, DataValue]], list[dict[str, DataValue]]]:
    system_columns: list[dict[str, DataValue]] = []
    non_system_columns: list[dict[str, DataValue]] = []
    for record in data:
        sys_cols, non_sys_cols = split_fields_with_prefixes(
            record, [constants.META_PREFIX, constants.DATAGRAM_PREFIX]
        )
        system_columns.append(sys_cols)
        non_system_columns.append(non_sys_cols)
    return system_columns, non_system_columns


class DictSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    def __init__(
        self,
        data: Collection[dict[str, DataValue]],
        tag_columns: Collection[str],
        tag_schema: Mapping[str, type] | None = None,
        packet_schema: Mapping[str, type] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        data = list(data)
        tags = []
        packets = []
        for item in data:
            tags.append({k: item[k] for k in tag_columns})
            packets.append({k: item[k] for k in item if k not in tag_columns})

        # TODO: visit source info logic
        source_info = ":".join(self.kernel_id)

        raw_data, system_data = split_system_columns(data)

        self.tags = tags
        self.packets = [add_source_field(packet, source_info) for packet in packets]

        self.tag_schema = (
            dict(tag_schema)
            if tag_schema
            else infer_python_schema_from_pylist_data(self.tags)
        )
        self.packet_schema = (
            dict(packet_schema)
            if packet_schema
            else infer_python_schema_from_pylist_data(self.packets)
        )

    def source_identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            tuple(self.tag_schema.items()),
            tuple(self.packet_schema.items()),
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
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
