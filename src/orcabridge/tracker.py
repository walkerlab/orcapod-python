import matplotlib.pyplot as plt
import networkx as nx

from orcabridge.base import Invocation, Operation, Tracker


class GraphTracker(Tracker):
    """
    A tracker that records the invocations of operations and generates a graph
    of the invocations and their dependencies.
    """

    # Thread-local storage to track active trackers

    def __init__(self) -> None:
        super().__init__()
        self.invocation_lut: dict[Operation, list[Invocation]] = {}

    def record(self, invocation: Invocation) -> None:
        invocation_list = self.invocation_lut.setdefault(invocation.operation, [])
        if invocation not in invocation_list:
            invocation_list.append(invocation)

    def reset(self) -> dict[Operation, list[Invocation]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_invocations = self.invocation_lut
        self.invocation_lut = {}
        return recorded_invocations

    def generate_namemap(self) -> dict[Invocation, str]:
        namemap = {}
        for operation, invocations in self.invocation_lut.items():
            # if only one entry present, use the operation name alone
            if operation.label is not None:
                node_label = operation.label
            else:
                node_label = str(operation)
            if len(invocations) == 1:
                namemap[invocations[0]] = node_label
                continue
            # if multiple entries, use the operation name and index
            for idx, invocation in enumerate(invocations):
                namemap[invocation] = f"{node_label}_{idx}"
        return namemap

    def generate_graph(self):
        G = nx.DiGraph()

        # Add edges for each invocation
        for operation, invocations in self.invocation_lut.items():
            for invocation in invocations:
                for upstream in invocation.streams:
                    # if upstream.invocation is not in the graph, add it
                    if upstream.invocation not in G:
                        G.add_node(upstream.invocation)
                    G.add_edge(upstream.invocation, invocation, stream=upstream)

        return G

    def draw_graph(self):
        G = self.generate_graph()
        labels = self.generate_namemap()

        pos = nx.drawing.nx_agraph.graphviz_layout(G, prog="dot")
        nx.draw(
            G,
            pos,
            labels=labels,
            node_size=2000,
            node_color="lightblue",
            with_labels=True,
            font_size=10,
            font_weight="bold",
            arrowsize=20,
        )
        plt.tight_layout()
