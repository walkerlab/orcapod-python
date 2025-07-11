from orcapod.protocols import data_protocols as dp
from collections import defaultdict
from abc import ABC, abstractmethod


class BasicTrackerManager:
    def __init__(self) -> None:
        self._active_trackers: list[dp.Tracker] = []

    def register_tracker(self, tracker: dp.Tracker) -> None:
        """
        Register a new tracker in the system.
        This is used to add a new tracker to the list of active trackers.
        """
        if tracker not in self._active_trackers:
            self._active_trackers.append(tracker)

    def deregister_tracker(self, tracker: dp.Tracker) -> None:
        """
        Remove a tracker from the system.
        This is used to deactivate a tracker and remove it from the list of active trackers.
        """
        if tracker in self._active_trackers:
            self._active_trackers.remove(tracker)

    def get_active_trackers(self) -> list[dp.Tracker]:
        """
        Get the list of active trackers.
        This is used to retrieve the currently active trackers in the system.
        """
        return [t for t in self._active_trackers if t.is_active()]

    def record_kernel_invocation(
        self, kernel: dp.Kernel, upstreams: tuple[dp.Stream, ...]
    ) -> None:
        """
        Record the output stream of a kernel invocation in the tracker.
        This is used to track the computational graph and the invocations of kernels.
        """
        for tracker in self.get_active_trackers():
            tracker.record_kernel_invocation(kernel, upstreams)

    def record_pod_invocation(
        self, pod: dp.Pod, upstreams: tuple[dp.Stream, ...]
    ) -> None:
        """
        Record the output stream of a pod invocation in the tracker.
        This is used to track the computational graph and the invocations of pods.
        """
        for tracker in self.get_active_trackers():
            tracker.record_pod_invocation(pod, upstreams)


class AutoRegisteringContextBasedTracker(ABC):
    def __init__(self, tracker_manager: dp.TrackerManager | None = None) -> None:
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._active = False

    def set_active(self, active: bool = True) -> None:
        if active:
            self._tracker_manager.register_tracker(self)
        else:
            self._tracker_manager.deregister_tracker(self)
        self._active = active

    def is_active(self) -> bool:
        return self._active

    @abstractmethod
    def record_kernel_invocation(
        self, kernel: dp.Kernel, upstreams: tuple[dp.Stream, ...]
    ) -> None: ...

    @abstractmethod
    def record_pod_invocation(
        self, pod: dp.Pod, upstreams: tuple[dp.Stream, ...]
    ) -> None: ...

    def __enter__(self):
        self.set_active(True)
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.set_active(False)


class GraphTracker(AutoRegisteringContextBasedTracker):
    """
    A tracker that records the invocations of operations and generates a graph
    of the invocations and their dependencies.
    """

    # Thread-local storage to track active trackers

    def __init__(self, tracker_manager: dp.TrackerManager | None = None) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self.kernel_to_invoked_stream_lut: dict[dp.Kernel, list[dp.Stream]] = (
            defaultdict(list)
        )

    def record(self, stream: dp.Stream) -> None:
        assert stream.source is not None, (
            "Stream must have a source kernel when recording."
        )
        stream_list = self.kernel_to_invoked_stream_lut[stream.source]
        if stream not in stream_list:
            stream_list.append(stream)

    def reset(self) -> dict[dp.Kernel, list[dp.Stream]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_streams = self.kernel_to_invoked_stream_lut
        self.kernel_to_invoked_stream_lut = defaultdict(list)
        return recorded_streams

    def generate_graph(self):
        import networkx as nx

        G = nx.DiGraph()

        # Add edges for each invocation
        for _, streams in self.kernel_to_invoked_stream_lut.items():
            for stream in streams:
                if stream not in G:
                    G.add_node(stream)
                for upstream in stream.upstreams:
                    G.add_edge(upstream, stream)
        return G

    # def generate_namemap(self) -> dict[Invocation, str]:
    #     namemap = {}
    #     for kernel, invocations in self.invocation_lut.items():
    #         # if only one entry present, use the kernel name alone
    #         if kernel.label is not None:
    #             node_label = kernel.label
    #         else:
    #             node_label = str(kernel)
    #         if len(invocations) == 1:
    #             namemap[invocations[0]] = node_label
    #             continue
    #         # if multiple entries, use the kernel name and index
    #         for idx, invocation in enumerate(invocations):
    #             namemap[invocation] = f"{node_label}_{idx}"
    #     return namemap

    # def draw_graph(self):
    #     import networkx as nx
    #     import matplotlib.pyplot as plt

    #     G = self.generate_graph()
    #     labels = self.generate_namemap()

    #     pos = nx.drawing.nx_agraph.graphviz_layout(G, prog="dot")
    #     nx.draw(
    #         G,
    #         pos,
    #         labels=labels,
    #         node_size=2000,
    #         node_color="lightblue",
    #         with_labels=True,
    #         font_size=10,
    #         font_weight="bold",
    #         arrowsize=20,
    #     )
    #     plt.tight_layout()


DEFAULT_TRACKER_MANAGER = BasicTrackerManager()
