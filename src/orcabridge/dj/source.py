from ..source import Source
from .stream import QueryStream, TableCachedStream, TableStream
from .operation import QueryOperation
from ..stream import SyncStream
from datajoint import Table
from typing import Any, Collection, Union, Optional
from datajoint import Schema
import datajoint as dj
from ..utils.name import pascal_to_snake, snake_to_pascal
from ..utils.stream_utils import common_elements
import logging
from orcabridge.hashing import hash_to_uuid

logger = logging.getLogger(__name__)


class QuerySource(Source, QueryOperation):
    """
    A speical type of source that returns and works with QueryStreams
    """


class TableSource(QuerySource):
    """
    A source that reads from a table.
    """

    def __init__(
        self, table: Union[Table, type[Table]], label: Optional[str] = None
    ) -> None:
        super().__init__(label=label)
        # if table is an instance, grab the class for consistency
        if not isinstance(table, type):
            table = table.__class__
        self.table = table

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            str(self.table.full_table_name),
        ) + tuple(streams)

    @property
    def label(self) -> str:
        if self._label is None:
            return self.table.__name__
        return self._label

    def forward(self, *streams: SyncStream) -> QueryStream:
        """
        Read from the table and return a stream of packets.
        """
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableSource")
        # make sure to pass in an instance of the table for the query
        return QueryStream(self.table(), [self.table()])

    def proj(self, *args, **kwargs) -> "TableSource":
        """
        Project the table and return a new source.
        """
        return TableSource(self.table.proj(*args, **kwargs))

    def __and__(self, other: Any) -> "TableSource":
        """
        Join the table with another table and return a new source.
        """
        if isinstance(other, TableSource):
            other = other.table
        elif isinstance(other, QueryStream):
            other = other.query
        else:
            raise ValueError(f"Object of type {type(other)} is not supported.")
        return TableSource(self.table & other)

    def __repr__(self):
        return self.table().__repr__()

    def preview(self, limit=None, width=None):
        return self.table().preview(limit=limit, width=width)

    def _repr_html_(self):
        """:return: HTML to display table in Jupyter notebook."""
        return self.table()._repr_html_()


class TableCachedStreamSource(QuerySource):
    """
    This class wraps any `Stream` and caches the output into a DataJoint table.
    The class instance acts as a source and returns a `QueryStream` when invoked.
    """

    def __init__(
        self,
        stream: SyncStream,
        schema: Schema,
        table_name: str | None = None,
        label: str | None = None,
    ):
        super().__init__(label=label)
        self.stream = stream
        self.schema = schema
        # if table name is not provided, use the name of the stream source
        if table_name is None:
            if stream.invocation is not None:
                table_name = stream.invocation.operation.__class__.__name__
            else:
                table_name = stream.__class__.__name__
        # make sure the table name is in snake case
        self.table_name = pascal_to_snake(table_name)

        self.table = None

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.stream,
            str(self.schema),
            self.table_name,
        ) + tuple(streams)

    @property
    def label(self) -> str:
        if self._label is None:
            if (
                hasattr(self.stream.invocation, "label")
                and self.stream.invocation.label is not None
            ):
                return self.stream.invocation.label
            else:
                return snake_to_pascal(self.table_name)
        return self._label

    def compile(self, tag_keys: Collection[str], packet_keys: Collection[str]) -> None:
        # create a table to store the cached packets

        key_fields = "\n".join([f"{k}: varchar(255)" for k in tag_keys])
        output_fields = "\n".join([f"{k}: varchar(255)" for k in packet_keys])

        class CachedTable(dj.Manual):
            source = self  # this refers to the outer class instance
            definition = f"""
            # {self.table_name} outputs
            {key_fields}
            ---
            {output_fields}
            """

        CachedTable.__name__ = snake_to_pascal(self.table_name)
        CachedTable = self.schema(CachedTable)
        self.table = CachedTable

    def forward(self, *streams: QueryStream) -> QueryStream:
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableCachedStreamSource")

        if self.table is None:
            # TODO: consider handling this lazily
            self.compile(*self.stream.keys())

        return TableCachedStream(self.table, self.stream)


class TableCachedSource(QuerySource):
    """
    This class wraps any `Source` and caches the output into a DataJoint table.
    Consequently, the table returns a `QueryStream` that can be used by any downstraem
    processes that relies on DJ-based streams (e.g. `TableCachedPod`).
    """

    def __init__(
        self,
        source: Source,
        schema: Schema,
        table_name: str = None,
        table_postfix: str = "",
        label: Optional[str] = None,
    ):
        super().__init__(label=label)
        self.source = source
        self.schema = schema
        # if table name is not provided, use the name of the source
        self.table_name = (
            table_name
            if table_name is not None
            else pascal_to_snake(source.__class__.__name__)
        ) + (f"_{table_postfix}" if table_postfix else "")
        self.table = None

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.source,
            str(self.schema),
            self.table_name,
        ) + tuple(streams)

    @property
    def label(self) -> str:
        if self._label is None:
            return self.source.label
        return self._label

    def compile(self) -> None:
        # create a table to store the cached packets
        tag_keys, packet_keys = self.source().keys()
        key_fields = "\n".join([f"{k}: varchar(255)" for k in tag_keys])
        output_fields = "\n".join([f"{k}: varchar(255)" for k in packet_keys])

        class CachedTable(dj.Manual):
            source = self  # this refers to the outer class instance
            definition = f"""
            # {self.table_name} outputs
            {key_fields}
            ---
            {output_fields}
            """

            def populate(
                self, batch_size: int = 10, use_skip_duplicates: bool = False
            ) -> int:
                return sum(
                    1
                    for _ in self.source(
                        batch_size=batch_size,
                        use_skip_duplicates=use_skip_duplicates,
                    )
                )

        CachedTable.__name__ = snake_to_pascal(self.table_name)
        CachedTable = self.schema(CachedTable)
        self.table = CachedTable

    def forward(
        self,
        *streams: QueryStream,
        batch_size: int = 10,
        use_skip_duplicates: bool = False,
    ) -> QueryStream:
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableCachedSource")

        if self.table is None:
            self.compile()
        return TableCachedStream(
            self.table,
            self.source(),
            batch_size=batch_size,
            use_skip_duplicates=use_skip_duplicates,
        )


class MergedQuerySource(QuerySource):
    """
    A source that represents multiple merged query.
    """

    def __init__(
        self,
        *streams: QueryStream,
        schema: Schema,
        table_name: str = None,
        table_postfix: str = "",
        label: Optional[str] = None,
        lazy_build: bool = True,
    ) -> None:
        super().__init__(label=label)
        self.streams = streams
        self.schema = schema
        self.table = None
        if table_name is None:
            table_name = self.label if self.label is not None else "MergedData"

        self.table_name = pascal_to_snake(table_name) + (
            f"_{table_postfix}" if table_postfix else ""
        )
        if not lazy_build:
            self.compile()

    @property
    def label(self) -> str:
        if self._label is None:
            return "_".join([stream.label for stream in self.streams])
        return self._label

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.streams,
        ) + tuple(streams)

    def forward(self, *streams: SyncStream) -> QueryStream:
        if len(streams) > 0:
            logger.warning(
                "Handling multiple streams in forward is not implemented yet in "
                "MergedQuerySource and this will be silently ignored"
            )
        if self.table is None:
            self.compile()

        return TableStream(self.table)

    def compile(self) -> None:
        part_tag_keys = []
        part_packet_keys = []
        for stream in self.streams:
            tag_key, packet_key = stream.keys()
            part_tag_keys.append(tag_key)
            part_packet_keys.append(packet_key)

        # find common keys among all tags and use that as primary key
        common_tag_keys = common_elements(*part_tag_keys)
        common_packet_keys = common_elements(*part_packet_keys)

        use_uuid = True
        if all([len(k) == len(common_tag_keys) for k in part_tag_keys]):
            # if all tags have the same number of keys, it is not necessary
            # to include an additional UUID
            use_uuid = False

        # create a table to store the cached packets
        key_fields = "\n".join([f"{k}: varchar(255)" for k in common_tag_keys])
        output_fields = "\n".join([f"{k}: varchar(255)" for k in common_packet_keys])
        table_field = f"{self.table_name}_part"
        uuid_field = f"{self.table_name}_uuid" if use_uuid else ""
        table_entry = f"{table_field}: varchar(255)"
        uuid_entry = f"{uuid_field}: uuid" if use_uuid else ""

        class MergedTable(dj.Manual):
            source = self
            definition = f"""
            # {self.table_name} inputs
            {key_fields}
            {table_entry}
            {uuid_entry}
            ---
            {output_fields}
            """

        for stream in self.streams:
            if not isinstance(stream, QueryStream):
                raise ValueError(
                    f"Stream {stream} is not a QueryStream. "
                    "Please use a QueryStream as input."
                )
            part_table = make_part_table(
                stream,
                common_tag_keys,
                common_packet_keys,
                table_field,
                uuid_field,
            )
            setattr(MergedTable, snake_to_pascal(stream.label), part_table)

        MergedTable.__name__ = snake_to_pascal(self.table_name)
        MergedTable = self.schema(MergedTable)
        self.table = MergedTable

    #     class CachedTable(dj.Manual):
    #         source = self  # this refers to the outer class instance
    #         definition = f"""
    #         # {self.table_name} outputs
    #         {key_fields}
    #         ---
    #         {output_fields}
    #         """

    #         def populate(
    #             self, batch_size: int = 10, use_skip_duplicates: bool = False
    #         ) -> int:
    #             return sum(
    #                 1
    #                 for _ in self.operation(
    #                     batch_size=batch_size,
    #                     use_skip_duplicates=use_skip_duplicates,
    #                 )
    #             )

    #     CachedTable.__name__ = snake_to_pascal(self.table_name)
    #     CachedTable = self.schema(CachedTable)
    #     self.table = CachedTable


def make_part_table(
    stream: QueryStream,
    common_tag_keys,
    common_packet_keys,
    table_field,
    uuid_field,
) -> type[dj.Part]:
    upstreams = "\n".join(
        f"-> self.upstream_tables[{i}]" for i in range(len(stream.upstream_tables))
    )

    tag_keys, packet_keys = stream.keys()

    extra_packet_keys = [k for k in packet_keys if k not in common_packet_keys]

    extra_output_fields = "\n".join([f"{k}: varchar(255)" for k in extra_packet_keys])

    class PartTable(dj.Part, dj.Computed):
        upstream_tables = stream.upstream_tables
        definition = f"""
        -> master
        ---
        {upstreams}
        {extra_output_fields}
        """

        @property
        def key_source(self):
            return stream.query

        def make(self, key):
            content = (stream.query & key).fetch1()
            content[table_field] = self.__class__.__name__
            if uuid_field:
                content[uuid_field] = hash_to_uuid(key)
            self.master.insert1(content, ignore_extra_fields=True)
            self.insert1(content, ignore_extra_fields=True)

    PartTable.__name__ = snake_to_pascal(stream.label)
    return PartTable
