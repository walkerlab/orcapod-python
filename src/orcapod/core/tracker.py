from orcapod.core.base import Invocation, Kernel, Tracker, SyncStream, Source
from orcapod.types import Tag, Packet, TypeSpec
from collections.abc import Collection, Iterator
from typing import Any

class StreamWrapper(SyncStream):
    """
    A wrapper for a SyncStream that allows it to be used as a Source.
    This is useful for cases where you want to treat a stream as a source
    without modifying the original stream.
    """

    def __init__(self, stream: SyncStream, **kwargs):
        super().__init__(**kwargs)
        self.stream = stream

    def keys(self, *streams: SyncStream, **kwargs) -> tuple[Collection[str]|None, Collection[str]|None]:
        return self.stream.keys(*streams, **kwargs)

    def types(self, *streams: SyncStream, **kwargs) -> tuple[TypeSpec|None, TypeSpec|None]:
        return self.stream.types(*streams, **kwargs)
    
    def computed_label(self) -> str | None:
        return self.stream.label

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Iterate over the stream, yielding tuples of (tags, packets).
        """
        yield from self.stream
        
    

class StreamSource(Source):
    def __init__(self, stream: SyncStream, **kwargs):
        super().__init__(skip_tracking=True, **kwargs)
        self.stream = stream

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "StreamSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )
        return StreamWrapper(self.stream)
    
    def identity_structure(self, *streams) -> Any:
        if len(streams) != 0:
            raise ValueError(
                "StreamSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )
         
        return (self.__class__.__name__, self.stream)

    def types(self, *streams: SyncStream, **kwargs) -> tuple[TypeSpec|None, TypeSpec|None]:
        return self.stream.types()
    
    def keys(self, *streams: SyncStream, **kwargs) -> tuple[Collection[str]|None, Collection[str]|None]:
        return self.stream.keys()

    def computed_label(self) -> str | None:
        return self.stream.label
    


class GraphTracker(Tracker):
    """
    A tracker that records the invocations of operations and generates a graph
    of the invocations and their dependencies.
    """

    # Thread-local storage to track active trackers

    def __init__(self) -> None:
        super().__init__()
        self.invocation_lut: dict[Kernel, list[Invocation]] = {}

    def record(self, invocation: Invocation) -> None:
        invocation_list = self.invocation_lut.setdefault(invocation.kernel, [])
        if invocation not in invocation_list:
            invocation_list.append(invocation)

    def reset(self) -> dict[Kernel, list[Invocation]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_invocations = self.invocation_lut
        self.invocation_lut = {}
        return recorded_invocations

    def generate_namemap(self) -> dict[Invocation, str]:
        namemap = {}
        for kernel, invocations in self.invocation_lut.items():
            # if only one entry present, use the kernel name alone
            if kernel.label is not None:
                node_label = kernel.label
            else:
                node_label = str(kernel)
            if len(invocations) == 1:
                namemap[invocations[0]] = node_label
                continue
            # if multiple entries, use the kernel name and index
            for idx, invocation in enumerate(invocations):
                namemap[invocation] = f"{node_label}_{idx}"
        return namemap

    def generate_graph(self):
        import networkx as nx

        G = nx.DiGraph()

        # Add edges for each invocation
        for kernel, invocations in self.invocation_lut.items():
            for invocation in invocations:
                for upstream in invocation.streams:
                    # if upstream.invocation is not in the graph, add it
                    upstream_invocation = upstream.invocation
                    if upstream_invocation is None:
                        # If upstream is None, create a stub kernel
                        upstream_invocation = Invocation(StreamSource(upstream), [])
                    if upstream_invocation not in G:
                        G.add_node(upstream_invocation)
                    G.add_edge(upstream_invocation, invocation, stream=upstream)

        return G

    def draw_graph(self):
        import networkx as nx
        import matplotlib.pyplot as plt

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
