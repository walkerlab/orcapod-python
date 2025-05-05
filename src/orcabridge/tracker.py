import threading
from typing import Dict, Collection, List
import networkx as nx
from .base import Operation, Invocation
import matplotlib.pyplot as plt


class Tracker:

    # Thread-local storage to track active trackers
    _local = threading.local()

    def __init__(self) -> None:
        self.active = False
        self.invocation_lut: Dict[Operation, Collection[Invocation]] = {}

    def record(self, invocation: Invocation) -> None:
        self.invocation_lut.setdefault(invocation.operation, set()).add(invocation)

    def reset(self) -> Dict[Operation, Collection[Invocation]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_invocations = self.invocation_lut
        self.invocation_lut = {}
        return recorded_invocations

    def generate_namemap(self) -> Dict[Invocation, str]:
        namemap = {}
        for operation, invocations in self.invocation_lut.items():
            # if only one entry present, use the operation name alone
            invocations = sorted(invocations)
            if len(invocations) == 1:
                namemap[invocations[0]] = f"{operation}"
                continue
            # if multiple entries, use the operation name and index
            for idx, invocation in enumerate(invocations):
                namemap[invocation] = f"{operation}_{idx}"
        return namemap

    def activate(self) -> None:
        """
        Activate the tracker. This is a no-op if the tracker is already active.
        """
        if not self.active:
            if not hasattr(self._local, "active_trackers"):
                self._local.active_trackers = []
            self._local.active_trackers.append(self)
            self.active = True

    def deactivate(self) -> None:
        # Remove this tracker from active trackers
        if hasattr(self._local, "active_trackers") and self.active:
            self._local.active_trackers.remove(self)
            self.active = False

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

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.deactivate()

    @classmethod
    def get_active_trackers(cls) -> List["Tracker"]:
        if hasattr(cls._local, "active_trackers"):
            return cls._local.active_trackers
        return []
