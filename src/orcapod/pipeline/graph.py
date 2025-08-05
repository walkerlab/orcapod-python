from orcapod.data.trackers import GraphTracker, Invocation
from orcapod.pipeline.nodes import KernelNode, PodNode
from orcapod import contexts
from orcapod.protocols import data_protocols as dp
from orcapod.protocols import store_protocols as sp
from typing import Any
from collections.abc import Collection
from orcapod.data.streams import WrappedStream
import logging


logger = logging.getLogger(__name__)


class Pipeline(GraphTracker):
    """
    Represents a pipeline in the system.
    This class extends GraphTracker to manage the execution of kernels and pods in a pipeline.
    """

    def __init__(
        self,
        name: str | tuple[str, ...],
        pipeline_store: sp.ArrowDataStore,
        results_store: sp.ArrowDataStore | None = None,
        tracker_manager: dp.TrackerManager | None = None,
        data_context: str | contexts.DataContext | None = None,
        auto_compile: bool = True,
    ):
        super().__init__(tracker_manager=tracker_manager, data_context=data_context)
        if not isinstance(name, tuple):
            name = (name,)
        self.name = name
        self.pipeline_store_path_prefix = self.name
        self.results_store_path_prefix = ()
        if results_store is None:
            if pipeline_store is None:
                raise ValueError(
                    "Either pipeline_store or results_store must be provided"
                )
            results_store = pipeline_store
            self.results_store_path_prefix = self.name + ("_results",)
        self.pipeline_store = pipeline_store
        self.results_store = results_store
        self.nodes = {}
        self.auto_compile = auto_compile
        self._dirty = False
        self._topological_order = []  # Track order of invocations

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        """
        Exit the pipeline context, ensuring all nodes are properly closed.
        """
        super().__exit__(exc_type, exc_value, traceback)
        if self.auto_compile:
            self.compile()

    def flush(self) -> None:
        self.pipeline_store.flush()
        self.results_store.flush()

    def record_kernel_invocation(
        self,
        kernel: dp.Kernel,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None:
        super().record_kernel_invocation(kernel, upstreams, label)
        self._dirty = True

    def record_pod_invocation(
        self,
        pod: dp.Pod,
        upstreams: tuple[dp.Stream, ...],
        label: str | None = None,
    ) -> None:
        super().record_pod_invocation(pod, upstreams, label)
        self._dirty = True

    def compile(self) -> None:
        import networkx as nx

        invocation_to_stream_lut = {}
        G = self.generate_graph()
        topological_order = []
        for invocation in nx.topological_sort(G):
            input_streams = [
                invocation_to_stream_lut[parent] for parent in invocation.parents()
            ]
            node = self.wrap_invocation(invocation, new_input_streams=input_streams)
            topological_order.append(node)
            invocation_to_stream_lut[invocation] = node()
            self.nodes[node.label] = node
        self._topolical_order = topological_order

    def run(self, execution_engine: dp.ExecutionEngine | None = None) -> None:
        # TODO: perform more efficient traversal through the graph!
        for node in self._topological_order:
            node().run(execution_engine=execution_engine)
            self.flush()

    def wrap_invocation(
        self,
        invocation: Invocation,
        new_input_streams: Collection[dp.Stream],
    ) -> dp.Kernel:
        if invocation in self.invocation_to_pod_lut:
            pod = self.invocation_to_pod_lut[invocation]
            node = PodNode(
                pod=pod,
                input_streams=new_input_streams,
                result_store=self.results_store,
                record_path_prefix=self.results_store_path_prefix,
                pipeline_store=self.pipeline_store,
                pipeline_path_prefix=self.pipeline_store_path_prefix,
                label=invocation.label,
            )
        elif invocation in self.invocation_to_source_lut:
            source = self.invocation_to_source_lut[invocation]
            node = KernelNode(
                kernel=source,
                input_streams=new_input_streams,
                pipeline_store=self.pipeline_store,
                pipeline_path_prefix=self.pipeline_store_path_prefix,
                label=invocation.label,
            )
        else:
            node = KernelNode(
                kernel=invocation.kernel,
                input_streams=new_input_streams,
                pipeline_store=self.pipeline_store,
                pipeline_path_prefix=self.pipeline_store_path_prefix,
                label=invocation.label,
            )
        return node

    def __getattr__(self, item: str) -> Any:
        """Allow direct access to pipeline attributes."""
        if item in self.nodes:
            return self.nodes[item]
        raise AttributeError(f"Pipeline has no attribute '{item}'")

    def __dir__(self) -> list[str]:
        """Return a list of attributes and methods of the pipeline."""
        return list(super().__dir__()) + list(self.nodes.keys())

    def rename(self, old_name: str, new_name: str) -> None:
        """
        Rename a node in the pipeline.
        This will update the label and the internal mapping.
        """
        if old_name not in self.nodes:
            raise KeyError(f"Node '{old_name}' does not exist in the pipeline.")
        if new_name in self.nodes:
            raise KeyError(f"Node '{new_name}' already exists in the pipeline.")
        node = self.nodes[old_name]
        del self.nodes[old_name]
        node.label = new_name
        self.nodes[new_name] = node
        logger.info(f"Node '{old_name}' renamed to '{new_name}'")
