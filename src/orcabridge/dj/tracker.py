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
from collections import defaultdict


def convert_to_query_operation(
    operation: Operation,
    schema: Schema,
    table_name: str = None,
    table_postfix: str = "",
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
        return (
            TableCachedSource(
                operation,
                schema=schema,
                table_name=table_name,
                table_postfix=table_postfix,
            ),
            True,
        )

    if isinstance(operation, FunctionPod):
        return (
            TableCachedPod(
                operation,
                schema=schema,
                table_name=table_name,
                table_postfix=table_postfix,
                streams=upstreams,
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

    def generate_tables(self, schema: Schema, module_name="dj_tables") -> List:

        G = self.generate_graph()

        # create a new module and add the tables to it
        module = ModuleType(module_name)
        module.__name__ = module_name

        desired_labels_lut = defaultdict(list)
        node_lut = {}
        edge_lut = {}
        for invocation in nx.topological_sort(G):
            streams = [edge_lut.get(stream, stream) for stream in invocation.streams]
            new_node, converted = convert_to_query_operation(
                invocation.operation,
                schema,
                table_name=None,
                table_postfix=invocation.content_hash_int(),
                upstreams=streams,
            )

            node_lut[invocation] = new_node
            desired_labels_lut[new_node.label].append(new_node)

            if converted:
                output_stream = new_node(*streams)
                for edge in G.out_edges(invocation):
                    edge_lut[G.edges[edge]["stream"]] = output_stream

        # construct labels for the oprations
        node_label_lut = {}
        for label, nodes in desired_labels_lut.items():
            if len(nodes) > 1:
                for idx, node in enumerate(nodes):
                    node_label_lut[node] = f"{label}Id{idx}"
            else:
                node_label_lut[nodes[0]] = label
        # generate the new converted computation graph
        G_dj = nx.DiGraph()
        for invocation in G:
            G_dj.add_node(node_lut[invocation])

        for edge in G.edges:
            stream = G.edges[edge]["stream"]
            G_dj.add_edge(
                node_lut[edge[0]],
                node_lut[edge[1]],
                stream=edge_lut.get(stream, stream),
            )

        for op in G_dj:
            if hasattr(op, "table"):
                table = op.table
                table.__module__ = str(module)
                table_name = node_label_lut[op]
                setattr(module, table_name, table)

        setattr(module, "schema", schema)
        sys.modules[module_name] = module

        return G_dj, module
