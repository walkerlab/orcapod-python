
from .stream import QueryStream, TableStream, TableCachedStream
from ..name import pascal_to_snake, snake_to_pascal
from ..hash import hash_dict
from ..pod import Pod, FunctionPod, FunctionPodWithDirStorage
from ..types import PodFunction
from .mapper import JoinQuery
from ..operation import Operation
import datajoint as dj
from datajoint import Schema
from typing import Collection, Optional, Tuple, Union, Iterator
from ..types import Tag, Packet
from ..stream import SyncStream, SyncStreamFromGenerator
from datajoint.table import Table

import logging

logger = logging.getLogger(__name__)





class TableCachedPod(Pod):
    def __init__(self, fp: FunctionPod, schema: Schema, table_name: str = None, streams: Collection[QueryStream] = None, create_table: bool = True):
        self.fp = fp
        self.schema = schema
        self.table_name = table_name if table_name is not None else pascal_to_snake(fp.function.__name__)
        self.streams = streams if streams is not None else []
        self.table = None
        if create_table:
            self.compile()

    def compile(self) -> None:
        if not all(isinstance(s, QueryStream) for s in self.streams):
            raise ValueError("All streams must be QueryStreams")
        
        # assign all upstrem tables to a local list
        upstream_tables = [table for stream in self.streams for table in stream.upstream_tables]
        #upstreams = '\n'.join([f"-> self.streams[{i}].upstream_tables[{j}]" for i, stream in enumerate(self.streams) for j in range(len(stream.upstream_tables))])
        upstreams = "\n".join(f"-> upstream_tables[{i}]" for i in range(len(upstream_tables)))
        outputs = '\n'.join([f"{k}: varchar(255)" for k in self.fp.output_keys])

        outer_class = self
        class PodTable(dj.Computed):
            definition = f"""
            # {self.table_name} outputs
            {upstreams}
            ---
            {outputs}
            """

            @property
            def key_source(self):
                # get the upstream tables
                # TODO: form this out of outer_class's streams instead
                upstreams = self.parents(primary=True, as_objects=True)
                source = upstreams[0].proj()
                for table in upstreams[1:]:
                    source = source * table.proj()
                return source

            def get_source_query(self) -> Tuple[QueryStream, Collection[Table]]:
                upstreams = self.parents(primary=True, as_objects=True, foreign_key_info=True)

                # for each upstream table, get the primary key
                source = None
                parent_tables = []
                for table, info in upstreams:
                    parent_tables.append(table)
                    proj_table = table.proj(..., **info['attr_map'])
                    if source is None:
                        source = proj_table
                    else:
                        source = source * proj_table
                return source, parent_tables

            def make(self, key):
                source, parent_tables = self.get_source_query()  # Updated to use the new method
                
                # form QueryStream using key
                query_stream = QueryStream(source & key, parent_tables)

                for key, packet in outer_class.fp(query_stream):
                    key.update(packet)
                    self.insert1(key)
                    logger.info(f"Inserted key: {key}")


        PodTable.__name__ = snake_to_pascal(self.table_name)
        PodTable = self.schema(PodTable)
        self.table = PodTable()

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
            joined_streams = QueryStream(streams[0].query.proj(), streams[0].upstream_tables)

        source, parent_tables = self.table.get_source_query()

        # restrict the source using the passed in query
        source = source & joined_streams.query

        # form QueryStream using key
        query_stream = QueryStream(source, parent_tables)

        # set table to allow direct insert
        self.table._allow_insert = True

        return TableCachedStream(self.table, self.fp(query_stream))