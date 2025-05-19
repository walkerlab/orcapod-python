from .stream import QueryStream, TableStream, TableCachedStream
from ..utils.name import pascal_to_snake, snake_to_pascal
from .operation import QueryOperation
from ..pod import Pod, FunctionPod
from .source import QuerySource
from .mapper import JoinQuery
import datajoint as dj
from datajoint import Schema
from typing import Collection, Tuple, Optional
from datajoint.table import Table

import logging

logger = logging.getLogger(__name__)


class QueryPod(Pod, QueryOperation):
    """
    A special type of operation that returns and works with
    QueryStreams
    """


class TableCachedPod(QueryPod, QuerySource):
    def __init__(
        self,
        fp: FunctionPod,
        schema: Schema,
        table_name: str = None,
        table_postfix: str = "",
        streams: Collection[QueryStream] = None,
        create_table: bool = True,
        label: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.fp = fp
        self.schema = schema
        self.table_name = (
            table_name
            if table_name is not None
            else pascal_to_snake(fp.function.__name__)
        ) + (f"_{table_postfix}" if table_postfix else "")
        self.streams = streams if streams is not None else []
        self.table = None
        if create_table:
            self.compile()

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            str(self.schema),
            self.table_name,
            self.fp
        ) + tuple(self.streams)

    @property
    def label(self) -> str:
        if self._label is None:
            return snake_to_pascal(self.fp.function.__name__)
        return self._label

    def prepare_source_query(self) -> Tuple[QueryStream, Collection[Table]]:
        if len(self.streams) > 1:
            query_stream = JoinQuery()(*self.streams)
        else:
            query_stream = self.streams[0]

        source_query = query_stream.query

        upstream_tables = query_stream.upstream_tables

        return source_query, upstream_tables

    def compile(self) -> None:
        if not all(isinstance(s, QueryStream) for s in self.streams):
            raise ValueError("All streams must be QueryStreams")

        source_query, upstream_tables = self.prepare_source_query()

        # upstreams = '\n'.join([f"-> self.streams[{i}].upstream_tables[{j}]" for i, stream in enumerate(self.streams) for j in range(len(stream.upstream_tables))])
        upstreams = "\n".join(
            f"-> upstream_tables[{i}]" for i in range(len(upstream_tables))
        )
        outputs = "\n".join([f"{k}: varchar(255)" for k in self.fp.output_keys])

        class PodTable(dj.Computed):
            definition = f"""
            # {self.table_name} outputs
            {upstreams}
            ---
            {outputs}
            """

            source = self

            @property
            def key_source(self):
                return source_query

            def make(self, key):
                # form QueryStream using key
                query_stream = QueryStream(source_query & key, upstream_tables)

                for key, packet in self.source.fp(query_stream):
                    key.update(packet)
                    self.insert1(key)
                    logger.info(f"Inserted key: {key}")

        PodTable.__name__ = snake_to_pascal(self.table_name)
        PodTable = self.schema(PodTable)
        self.table = PodTable

    def forward(self, *streams: QueryStream) -> QueryStream:
        """
        This method has two modes of operations: as a source and as a pod.
        When used as a source, it will return a stream of data from what's already
        in the table. When used as a pod, it will execute the function as necessary
        and insert the results into the table.
        """

        # if no stream are provided, act as a source and return the table stream
        if len(streams) < 1:
            return TableStream(self.table)

        # verify that all stream are QueryStreams
        if not all(isinstance(s, QueryStream) for s in streams):
            raise ValueError("All streams must be QueryStreams")

        # be sure to project out non primary keys
        if len(streams) > 1:
            joined_streams = JoinQuery()(*streams, project=True)
        else:
            # TODO: add proj method onto QueryStream
            joined_streams = QueryStream(
                streams[0].query.proj(), streams[0].upstream_tables
            )

        source_query, upstream_tables = self.prepare_source_query()

        # restrict the source using the passed in query
        source_query = source_query & joined_streams.query

        # form QueryStream using key
        query_stream = QueryStream(source_query, upstream_tables)

        # set table to allow direct insert
        self.table._allow_insert = True

        return TableCachedStream(self.table, self.fp(query_stream))
