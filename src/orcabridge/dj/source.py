from ..source import Source
from .stream import QueryStream, TableCachedStream
from ..stream import SyncStream, SyncStreamFromGenerator
from datajoint import Table
from typing import Any, Collection, Union, Iterator, Tuple
from datajoint import Schema
import datajoint as dj
from ..types import Tag, Packet
from ..name import pascal_to_snake, snake_to_pascal


class TableSource(Source):
    """
    A source that reads from a table.
    """

    def __init__(self, table: Union[Table, type[Table]]) -> None:
        # if table is a class, instantiate it for consistency
        if isinstance(table, type):
            table = table()
        self.table = table

    def __call__(self, *streams: SyncStream) -> QueryStream:
        """
        Read from the table and return a stream of packets.
        """
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableSource")
        return QueryStream(self.table, [self.table])

    def proj(self, *args, **kwargs) -> 'TableSource':
        """
        Project the table and return a new source.
        """
        return TableSource(self.table.proj(*args, **kwargs))
    
    def __and__(self, other: Any) -> 'TableSource':
        """
        Join the table with another table and return a new source.
        """
        if isinstance(other, TableSource):
            other = other.table
        elif isinstance(other, QueryStream):
            other = other.query
        return TableSource(self.table & other)
    
    def __repr__(self):
        return self.table.__repr__()

    def preview(self, limit=None, width=None):
        return self.table.preview(self, limit=limit, width=width)

    def _repr_html_(self):
        """:return: HTML to display table in Jupyter notebook."""
        return self.table._repr_html_()
    
  
class TableCachedSource(Source):
    def __init__(self, source: Source, schema: Schema, table_name: str = None):
        self.source = source
        self.schema = schema
        # if table name is not provided, use the name of the source
        self.table_name = table_name if table_name is not None else pascal_to_snake(source.__class__.__name__)
        self.table = None

    def compile(self, tag_keys: Collection[str], packet_keys: Collection[str]) -> None:
        # create a table to store the cached packets
        key_fields = '\n'.join([f"{k}: varchar(255)" for k in tag_keys])
        output_fields = '\n'.join([f"{k}: varchar(255)" for k in packet_keys])

        outer_class = self
        class CachedTable(dj.Manual):
            definition = f"""
            # {self.table_name} outputs
            {key_fields}
            ---
            {output_fields}
            """

            def populate(self):
                return sum(1 for _ in outer_class())


        CachedTable.__name__ = snake_to_pascal(self.table_name)
        CachedTable = self.schema(CachedTable)
        self.table = CachedTable()

    def forward(self, *streams: QueryStream) -> QueryStream:
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableCachedSource")

        if self.table is None:
            self.compile(*self.source().keys())
        return TableCachedStream(self.table, self.source())

class TableCachedStreamSource(Source):
    def __init__(self, stream: SyncStream, schema: Schema, table_name: str = None):
        self.stream = stream
        self.schema = schema
        # if table name is not provided, use the name of the stream source
        self.table_name = table_name if table_name is not None else pascal_to_snake(stream.source.__class__.__name__)
        self.table = None

    def compile(self, tag_keys: Collection[str], packet_keys: Collection[str]) -> None:
        # create a table to store the cached packets

        key_fields = '\n'.join([f"{k}: varchar(255)" for k in tag_keys])
        output_fields = '\n'.join([f"{k}: varchar(255)" for k in packet_keys])

        class CachedTable(dj.Manual):
            definition = f"""
            # {self.table_name} outputs
            {key_fields}
            ---
            {output_fields}
            """

        CachedTable.__name__ = snake_to_pascal(self.table_name)
        CachedTable = self.schema(CachedTable)
        self.table = CachedTable()


    def forward(self, *streams: QueryStream) -> QueryStream:
        if len(streams) > 0:
            raise ValueError("No streams should be passed to TableCachedStreamSource")

        if self.table is None:
            # TODO: consider handling this lazily
            self.compile(*self.stream.keys())

        return TableCachedStream(self.table, self.stream)