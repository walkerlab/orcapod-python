import rustworkx as rx
from rustworkx.visualization import graphviz_draw


# Example orcherstrator logic for deployment and exeution of a pipeline
class Pod:
    def __init__(self, name):
        self.name = name
        self.parent = (
            []
        )  # List of nodes that must be completed before this node can start
        self.children = (
            []
        )  # List of nodes that is waiting on the completion of this node

    def subscribe(self, node):
        """
        Subscribe a node to this node's completion.

        :param node: The node that is waiting for this node to complete.
        """
        self.children.append(node)
        node.parent.append(self)

    def add_parent(self, parent_node):
        parent_node.subscribe(self)

    def add_child(self, node):
        """
        Add node to the DAG by subscribing it to this node's completion.
        NOTE: For RUST, this will be a trait that allows a call back for when this node completes.

        :param node: The node that is waiting for this node to complete.
        """
        self.subscribe(node)

    def draw_graph(self):
        dag = rx.PyDiGraph(check_cycle=True)
        for root_node in self.find_root_nodes():
            parent_idx = dag.add_node(root_node)
            root_node.add_children_to_dag(dag, parent_idx)

        return graphviz_draw(dag, node_attr_fn=lambda node: {"label": node.name})

    def add_children_to_dag(self, dag, parent_idx):
        """
        Add all children of this node to the DAG.

        :param dag: The directed acyclic graph (DAG) to which the children will be added.
        """
        for child in self.children:
            child_idx = dag.add_child(parent_idx, child, None)
            child.add_children_to_dag(dag, child_idx)

    def find_root_nodes(self):
        """
        Find all root nodes in the DAG.

        :return: A list of root nodes.
        """
        if self.parent.__len__() == 0:
            return [self]
        else:
            for parent in self.parent:
                return parent.find_root_nodes()


class PodJob:
    def __init__(self, pod):
        self.pod = pod


class PodRun:
    def __init__(self, pod_job):
        self.pod_job = pod_job


class PodResult:
    def __init__(self, pod_job, result):
        self.pod_job = pod_job
        self.result = result


class Pipeline:
    def __init__(self, root_nodes):
        self.root_nodes = root_nodes


class DockerOrchestrator:

    def start_blocking(self, pod_job):
        pass

    # Orchestrator worker functions
    def add_worker(self, worker):
        """
        Add a worker to the orchestrator.

        :param worker: The worker instance to be added.
        """
        self.workers.append(worker)

    def remove_worker(self, worker):
        """
        Remove a worker from the orchestrator.

        :param worker: The worker instance to be removed.
        """
        if worker in self.workers:
            self.workers.remove(worker)


class OrcaOrchestrator:
    # This deals with execution of pipelines and managing pods
    def __init__(self):
        self.workers = []
