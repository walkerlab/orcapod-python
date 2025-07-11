from orcapod.protocols import data_protocols as dp, hashing_protocols as hp
from orcapod.hashing.defaults import get_default_object_hasher
from collections import defaultdict
from abc import ABC, abstractmethod
from typing import Any, ContextManager, Generator
from contextlib import contextmanager


class BasicTrackerManager:
    def __init__(self) -> None:
        self._active_trackers: list[dp.Tracker] = []
        self._active = True

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the tracker manager.
        This is used to enable or disable the tracker manager.
        """
        self._active = active

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
        if not self._active:
            return []
        # Filter out inactive trackers
        # This is to ensure that we only return trackers that are currently active
        return [t for t in self._active_trackers if t.is_active()]

    def record_kernel_invocation(
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None:
        """
        Record the output stream of a kernel invocation in the tracker.
        This is used to track the computational graph and the invocations of kernels.
        """
        for tracker in self.get_active_trackers():
            tracker.record_kernel_invocation(kernel, upstreams, label=label)

    def record_pod_invocation(
        self, pod: dp.Pod, upstreams: tuple[dp.Stream, ...], label: str | None = None
    ) -> None:
        """
        Record the output stream of a pod invocation in the tracker.
        This is used to track the computational graph and the invocations of pods.
        """
        for tracker in self.get_active_trackers():
            tracker.record_pod_invocation(pod, upstreams, label=label)

    @contextmanager
    def no_tracking(self) -> Generator[None, Any, None]:
        original_state = self._active
        self.set_active(False)
        try:
            yield
        finally:
            self.set_active(original_state)


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
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None: ...

    @abstractmethod
    def record_pod_invocation(
        self, pod: dp.Pod, upstreams: tuple[dp.Stream, ...], label: str | None = None
    ) -> None: ...

    def __enter__(self):
        self.set_active(True)
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.set_active(False)


class Invocation:
    def __init__(
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None:
        """
        Represents an invocation of a kernel with its upstream streams.
        This is used to track the computational graph and the invocations of kernels.
        """
        self.kernel = kernel
        self.upstreams = upstreams
        self._label = label

    def parents(self) -> tuple["Invocation", ...]:
        parent_invoctions = []
        for stream in self.upstreams:
            if stream.source is not None:
                parent_invoctions.append(Invocation(stream.source, stream.upstreams))
        return tuple(parent_invoctions)

    @property
    def label(self) -> str | None:
        """
        Return the label of the kernel invocation.
        This is used to identify the invocation in the tracker.
        """
        return self._label or self.kernel.label or self.kernel.__class__.__name__

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this invocation.
        This is used to uniquely identify the invocation in the tracker.
        """
        return self.kernel.identity_structure(*self.upstreams)


class GraphTracker(AutoRegisteringContextBasedTracker):
    """
    A tracker that records the invocations of operations and generates a graph
    of the invocations and their dependencies.
    """

    # Thread-local storage to track active trackers

    def __init__(
        self,
        tracker_manager: dp.TrackerManager | None = None,
        object_hasher: hp.ObjectHasher | None = None,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        if object_hasher is None:
            object_hasher = get_default_object_hasher()
        self.object_hasher = object_hasher
        # Dictionary to map kernels to the streams they have invoked
        # This is used to track the computational graph and the invocations of kernels
        self.id_to_invocation_lut: dict[str, Invocation] = {}
        self.id_to_label_lut: dict[str, list[str]] = defaultdict(list)
        self.id_to_pod_lut: dict[str, dp.Pod] = {}

    def record(self, stream: dp.Stream) -> None:
        assert stream.source is not None, (
            "Stream must have a source kernel when recording."
        )
        stream_list = self.kernel_to_invoked_stream_lut[stream.source]
        if stream not in stream_list:
            stream_list.append(stream)

    def _record_kernel_and_get_id(
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> str:
        invocation = Invocation(kernel, upstreams, label=label)
        invocation_id = self.object_hasher.hash_to_hex(invocation)
        if invocation_id not in self.id_to_invocation_lut:
            self.id_to_invocation_lut[invocation_id] = invocation
        label = label or kernel.label or kernel.__class__.__name__
        existing_labels = self.id_to_label_lut[invocation_id]
        if label not in existing_labels:
            existing_labels.append(label)
        return invocation_id

    def record_kernel_invocation(
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None:
        """
        Record the output stream of a kernel invocation in the tracker.
        This is used to track the computational graph and the invocations of kernels.
        """
        self._record_kernel_and_get_id(kernel, upstreams, label)

    def record_pod_invocation(
        self, pod: dp.Pod, upstreams: tuple[dp.Stream, ...], label: str | None = None
    ) -> None:
        """
        Record the output stream of a pod invocation in the tracker.
        """
        invocation_id = self._record_kernel_and_get_id(pod, upstreams, label)
        self.id_to_pod_lut[invocation_id] = pod

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
