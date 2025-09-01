from orcapod.core.trackers import GraphTracker, Invocation
from orcapod.pipeline.nodes import KernelNode, PodNode
from orcapod.protocols.pipeline_protocols import Node
from orcapod import contexts
from orcapod.protocols import core_protocols as cp
from orcapod.protocols import database_protocols as dbp
from typing import Any
from collections.abc import Collection
import os
import tempfile
import logging
import asyncio
from typing import TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")


def synchronous_run(async_func, *args, **kwargs):
    """
    Use existing event loop if available.

    Pros: Reuses existing loop, more efficient
    Cons: More complex, need to handle loop detection
    """
    try:
        # Check if we're already in an event loop
        _ = asyncio.get_running_loop()

        def run_in_thread():
            return asyncio.run(async_func(*args, **kwargs))

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_in_thread)
            return future.result()
    except RuntimeError:
        # No event loop running, safe to use asyncio.run()
        return asyncio.run(async_func(*args, **kwargs))


logger = logging.getLogger(__name__)


class Pipeline(GraphTracker):
    """
    Represents a pipeline in the system.
    This class extends GraphTracker to manage the execution of kernels and pods in a pipeline.
    """

    def __init__(
        self,
        name: str | tuple[str, ...],
        pipeline_database: dbp.ArrowDatabase,
        results_database: dbp.ArrowDatabase | None = None,
        tracker_manager: cp.TrackerManager | None = None,
        data_context: str | contexts.DataContext | None = None,
        auto_compile: bool = True,
    ):
        super().__init__(tracker_manager=tracker_manager, data_context=data_context)
        if not isinstance(name, tuple):
            name = (name,)
        self.name = name
        self.pipeline_store_path_prefix = self.name
        self.results_store_path_prefix = ()
        if results_database is None:
            if pipeline_database is None:
                raise ValueError(
                    "Either pipeline_database or results_database must be provided"
                )
            results_database = pipeline_database
            self.results_store_path_prefix = self.name + ("_results",)
        self.pipeline_database = pipeline_database
        self.results_database = results_database
        self.nodes: dict[str, Node] = {}
        self.auto_compile = auto_compile
        self._dirty = False
        self._ordered_nodes = []  # Track order of invocations

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        """
        Exit the pipeline context, ensuring all nodes are properly closed.
        """
        super().__exit__(exc_type, exc_value, traceback)
        if self.auto_compile:
            self.compile()

    def flush(self) -> None:
        self.pipeline_database.flush()
        self.results_database.flush()

    def record_kernel_invocation(
        self,
        kernel: cp.Kernel,
        upstreams: tuple[cp.Stream, ...],
        label: str | None = None,
    ) -> None:
        super().record_kernel_invocation(kernel, upstreams, label)
        self._dirty = True

    def record_pod_invocation(
        self,
        pod: cp.Pod,
        upstreams: tuple[cp.Stream, ...],
        label: str | None = None,
    ) -> None:
        super().record_pod_invocation(pod, upstreams, label)
        self._dirty = True

    def compile(self) -> None:
        import networkx as nx

        name_candidates = {}

        invocation_to_stream_lut = {}
        G = self.generate_graph()
        node_graph = nx.DiGraph()
        for invocation in nx.topological_sort(G):
            input_streams = [
                invocation_to_stream_lut[parent] for parent in invocation.parents()
            ]

            node = self.wrap_invocation(invocation, new_input_streams=input_streams)
            for parent in node.upstreams:
                node_graph.add_edge(parent.source, node)

            invocation_to_stream_lut[invocation] = node()
            name_candidates.setdefault(node.label, []).append(node)

        # visit through the name candidates and resolve any collisions
        for label, nodes in name_candidates.items():
            if len(nodes) > 1:
                # If there are multiple nodes with the same label, we need to resolve the collision
                logger.info(f"Collision detected for label '{label}': {nodes}")
                for i, node in enumerate(nodes, start=1):
                    self.nodes[f"{label}_{i}"] = node
            else:
                self.nodes[label] = nodes[0]

        self.label_lut = {v: k for k, v in self.nodes.items()}

        self.graph = node_graph

    def show_graph(self, **kwargs) -> None:
        render_graph(self.graph, self.label_lut, **kwargs)

    def run(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        run_async: bool | None = None,
    ) -> None:
        """Execute the pipeline by running all nodes in the graph.

        This method traverses through all nodes in the graph and executes them sequentially
        using the specified execution engine. After execution, flushes the pipeline.

        Args:
            execution_engine (dp.ExecutionEngine | None): The execution engine to use for running
                the nodes. If None, creates a new default ExecutionEngine instance.
            run_async (bool | None): Whether to run nodes asynchronously. If None, defaults to
                the preferred mode based on the execution engine.

        Returns:
            None

        Note:
            Current implementation uses a simple traversal through all nodes. Future versions
            may implement more efficient graph traversal algorithms.
        """
        for node in self.nodes.values():
            if run_async:
                synchronous_run(node.run_async, execution_engine=execution_engine)
            else:
                node.run(execution_engine=execution_engine)

        self.flush()

    def wrap_invocation(
        self,
        invocation: Invocation,
        new_input_streams: Collection[cp.Stream],
    ) -> Node:
        if invocation in self.invocation_to_pod_lut:
            pod = self.invocation_to_pod_lut[invocation]
            node = PodNode(
                pod=pod,
                input_streams=new_input_streams,
                result_database=self.results_database,
                record_path_prefix=self.results_store_path_prefix,
                pipeline_database=self.pipeline_database,
                pipeline_path_prefix=self.pipeline_store_path_prefix,
                label=invocation.label,
            )
        elif invocation in self.invocation_to_source_lut:
            source = self.invocation_to_source_lut[invocation]
            node = KernelNode(
                kernel=source,
                input_streams=new_input_streams,
                pipeline_database=self.pipeline_database,
                pipeline_path_prefix=self.pipeline_store_path_prefix,
                label=invocation.label,
            )
        else:
            node = KernelNode(
                kernel=invocation.kernel,
                input_streams=new_input_streams,
                pipeline_database=self.pipeline_database,
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


# import networkx as nx
# # import graphviz
# import matplotlib.pyplot as plt
# import matplotlib.image as mpimg
# import tempfile
# import os


class GraphRenderer:
    """Simple renderer for NetworkX graphs using Graphviz DOT format"""

    def __init__(self):
        """Initialize the renderer"""
        pass

    def _sanitize_node_id(self, node_id: Any) -> str:
        """Convert node_id to a valid DOT identifier using hash"""
        return f"node_{hash(node_id)}"

    def _get_node_label(
        self, node_id: Any, label_lut: dict[Any, str] | None = None
    ) -> str:
        """Get label for a node"""
        if label_lut and node_id in label_lut:
            return label_lut[node_id]
        return str(node_id)

    def generate_dot(
        self,
        graph: "nx.DiGraph",
        label_lut: dict[Any, str] | None = None,
        rankdir: str = "TB",
        node_shape: str = "box",
        node_style: str = "filled",
        node_color: str = "lightblue",
        edge_color: str = "black",
        dpi: int = 150,
    ) -> str:
        """
        Generate DOT syntax from NetworkX graph

        Args:
            graph: NetworkX DiGraph to render
            label_lut: Optional dictionary mapping node_id -> display_label
            rankdir: Graph direction ('TB', 'BT', 'LR', 'RL')
            node_shape: Shape for all nodes
            node_style: Style for all nodes
            node_color: Fill color for all nodes
            edge_color: Color for all edges
            dpi: Resolution for rendered image (default 150)

        Returns:
            DOT format string
        """
        try:
            import graphviz
        except ImportError as e:
            raise ImportError(
                "Graphviz is not installed. Please install graphviz to render graph of the pipeline."
            ) from e

        dot = graphviz.Digraph(comment="NetworkX Graph")

        # Set graph attributes
        dot.attr(rankdir=rankdir, dpi=str(dpi))
        dot.attr("node", shape=node_shape, style=node_style, fillcolor=node_color)
        dot.attr("edge", color=edge_color)

        # Add nodes
        for node_id in graph.nodes():
            sanitized_id = self._sanitize_node_id(node_id)
            label = self._get_node_label(node_id, label_lut)
            dot.node(sanitized_id, label=label)

        # Add edges
        for source, target in graph.edges():
            source_id = self._sanitize_node_id(source)
            target_id = self._sanitize_node_id(target)
            dot.edge(source_id, target_id)

        return dot.source

    def render_graph(
        self,
        graph: nx.DiGraph,
        label_lut: dict[Any, str] | None = None,
        show: bool = True,
        output_path: str | None = None,
        raw_output: bool = False,
        rankdir: str = "TB",
        figsize: tuple = (6, 4),
        dpi: int = 150,
        **style_kwargs,
    ) -> str | None:
        """
        Render NetworkX graph using Graphviz

        Args:
            graph: NetworkX DiGraph to render
            label_lut: Optional dictionary mapping node_id -> display_label
            show: Display the graph using matplotlib
            output_path: Save graph to file (e.g., 'graph.png', 'graph.pdf')
            raw_output: Return DOT syntax instead of rendering
            rankdir: Graph direction ('TB', 'BT', 'LR', 'RL')
            figsize: Figure size for matplotlib display
            dpi: Resolution for rendered image (default 150)
            **style_kwargs: Additional styling (node_color, edge_color, node_shape, etc.)

        Returns:
            DOT syntax if raw_output=True, None otherwise
        """
        try:
            import graphviz
        except ImportError as e:
            raise ImportError(
                "Graphviz is not installed. Please install graphviz to render graph of the pipeline."
            ) from e

        if raw_output:
            return self.generate_dot(graph, label_lut, rankdir, dpi=dpi, **style_kwargs)

        # Create Graphviz object
        dot = graphviz.Digraph(comment="NetworkX Graph")
        dot.attr(rankdir=rankdir, dpi=str(dpi))

        # Apply styling
        node_shape = style_kwargs.get("node_shape", "box")
        node_style = style_kwargs.get("node_style", "filled")
        node_color = style_kwargs.get("node_color", "lightblue")
        edge_color = style_kwargs.get("edge_color", "black")

        dot.attr("node", shape=node_shape, style=node_style, fillcolor=node_color)
        dot.attr("edge", color=edge_color)

        # Add nodes with labels
        for node_id in graph.nodes():
            sanitized_id = self._sanitize_node_id(node_id)
            label = self._get_node_label(node_id, label_lut)
            dot.node(sanitized_id, label=label)

        # Add edges
        for source, target in graph.edges():
            source_id = self._sanitize_node_id(source)
            target_id = self._sanitize_node_id(target)
            dot.edge(source_id, target_id)

        # Handle output
        if output_path:
            # Save to file
            name, ext = os.path.splitext(output_path)
            format_type = ext[1:] if ext else "png"
            dot.render(name, format=format_type, cleanup=True)
            print(f"Graph saved to {output_path}")

        if show:
            # Display with matplotlib
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                dot.render(tmp.name[:-4], format="png", cleanup=True)

                import matplotlib.pyplot as plt
                import matplotlib.image as mpimg

                # Display with matplotlib
                img = mpimg.imread(tmp.name)
                plt.figure(figsize=figsize)
                plt.imshow(img)
                plt.axis("off")
                plt.title("Graph Visualization")
                plt.tight_layout()
                plt.show()

                # Clean up
                os.unlink(tmp.name)

        return None


# Convenience function for quick rendering
def render_graph(
    graph: nx.DiGraph, label_lut: dict[Any, str] | None = None, **kwargs
) -> str | None:
    """
    Convenience function to quickly render a NetworkX graph

    Args:
        graph: NetworkX DiGraph to render
        label_lut: Optional dictionary mapping node_id -> display_label
        **kwargs: All other arguments passed to GraphRenderer.render_graph()

    Returns:
        DOT syntax if raw_output=True, None otherwise
    """
    renderer = GraphRenderer()
    return renderer.render_graph(graph, label_lut, **kwargs)
