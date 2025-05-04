from ..tracker import Tracker
from datajoint import Schema
from typing import List, Collection, Tuple, Optional
from types import ModuleType
import networkx as nx


from ..base import Operation, Source
from ..mapper import Mapper
from ..pod import FunctionPod
from .stream import QueryStream
from .source import TableCachedSource
from .operation import QueryOperation
from .pod import TableCachedPod
from .mapper import convert_to_query_mapper
import datajoint as dj
import sys


def convert_to_query_operation(
    operation: Operation,
    schema: Schema,
    table_name: str = None,
    upstreams: Optional[Collection[QueryStream]] = None,
) -> Tuple[QueryOperation, bool]:
    """
    Convert a generic operation to an equivalent, DataJoint specific operation
    """
    if upstreams is None:
        upstreams = []

    if isinstance(operation, QueryOperation):
        return operation, False

    if isinstance(operation, Source) and len(upstreams) == 0:
        return TableCachedSource(operation, schema=schema, table_name=table_name), True

    if isinstance(operation, FunctionPod):
        return (
            TableCachedPod(
                operation, schema=schema, table_name=table_name, streams=upstreams
            ),
            True,
        )

    if isinstance(operation, Mapper):
        return convert_to_query_mapper(operation), True

    # operation conversion is not supported, raise an error
    raise ValueError(f"Unsupported operation for DJ conversion: {operation}")


class QueryTracker(Tracker):
    """
    Query-specific tracker that tracks the invocations of operations
    and their associated streams.
    """

    def __init__(self) -> None:
        super().__init__()
        self._converted_graph = None

    def generate_tables(self, schema: Schema) -> List:

        G = self.generate_graph()

        node_lut = {}
        edge_lut = {}
        for node in nx.topological_sort(G):
            # replace edges(streams) with updated streams if present
            streams = [edge_lut.get(stream, stream) for stream in node.streams]
            new_node, converted = convert_to_query_operation(
                node.operation, schema, table_name=None, upstreams=streams
            )

            node_lut[node] = new_node

            if converted:
                output_stream = new_node(*streams)
                for edge in G.out_edges(node):
                    edge_lut[G.edges[edge]["stream"]] = output_stream

        # generate the new converted computation graph
        G_dj = nx.DiGraph()
        for node in G:
            G_dj.add_node(node_lut[node])

        for edge in G.edges:
            stream = G.edges[edge]["stream"]
            G_dj.add_edge(
                node_lut[edge[0]],
                node_lut[edge[1]],
                stream=edge_lut.get(stream, stream),
            )

        # create a new module and add the tables to it
        module = ModuleType("dj_tables")
        module.__name__ = "dj_tables"

        for node in G_dj:
            if hasattr(node, "table"):
                print(node.table)
                table = node.table
                table.__module__ = module
                table_name = table.__name__
                setattr(module, table_name, table)

        sys.modules["dj_tables"] = module

        return G_dj, module
