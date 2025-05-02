from ..base import Tracker
from datajoint import Schema
from typing import List, Collection
import networkx as nx


from ..source import Source
from ..stream import SyncStream
from ..mapper import Join, MapKeys, MapTags
from ..base import Operation
from ..pod import Pod, FunctionPod
from .stream import QueryStream
from .source import TableCachedSource
from .pod import TableCachedPod
from .mapper import JoinQuery, RestrictQuery, ProjectQuery
import datajoint as dj
from ..types import Tag, Packet


def wrap_with_table(
    operation: Operation,
    streams: Collection[QueryStream],
    schema: dj.Schema,
    table_name: str,
):

    if isinstance(operation, Source) and len(streams) == 0:
        return TableCachedSource(operation, schema, table_name)

    if isinstance(operation, FunctionPod):
        return TableCachedPod(operation, schema, table_name, streams=streams)

    if isinstance(operation, Join):
        return JoinQuery()

    if isinstance(operation, MapKeys):
        proj_map = {v: k for k, v in operation.key_map.items()}
        return ProjectQuery(..., **proj_map)

    if isinstance(operation, MapTags):
        proj_map = {v: k for k, v in operation.tag_map.items()}
        args = [] if operation.drop_unmapped else [...]
        return ProjectQuery(*args, **proj_map)

    print(f"Unknown operation: {operation}, passing back as is")
    return operation


class DJTracker(Tracker):
    """
    DataJoint specific tracker that tracks the invocations of operations
    and their associated streams.
    """

    def __init__(self) -> None:
        super().__init__()

    def generate_tables(self, schema: Schema) -> List:

        G = self.generate_graph()

        tables = []
        node_lut = {}
        edge_lut = {}
        for node in nx.topological_sort(G):
            # replace edges(streams) with updated streams if present
            streams = [edge_lut.get(stream, stream) for stream in node.streams]

            new_node = wrap_with_table(node.operation, streams, schema, table_name=None)

            node_lut[node] = new_node
            output_stream = new_node(*streams)
            if isinstance(new_node, (TableCachedSource, TableCachedPod)):
                tables.append(new_node.table)
            for edge in G.out_edges(node):
                edge_lut[G.edges[edge]["stream"]] = output_stream
        return tables
