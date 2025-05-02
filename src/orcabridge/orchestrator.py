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

    def add_subscriber(self, node):
        """
        Subscribe a node to this node's completion.

        :param node: The node that is waiting for this node to complete.
        """
        self.children.append(node)
        node.parent.append(self)

    def node_exist_in_chain(self, target_node):
        return sum(
            [
                root_node.node_exist_in_children_chain(target_node, 0)
                for root_node in self.find_root_nodes()
            ]
        )

    def node_exist_in_children_chain(self, target_node, num_matches=0):
        if self.name.split("<")[0] == target_node.name:
            return 1
        else:
            return sum(
                [
                    child_node.node_exist_in_children_chain(target_node, num_matches)
                    for child_node in self.children
                ]
            )

    def add_parent(self, parent_node):
        parent_node.add_subscriber(
            self.add_numeration(parent_node.node_exist_in_chain(self))
        )
        return parent_node

    def add_child(self, node):
        """
        Add node to the DAG by subscribing it to this node's completion.
        NOTE: For RUST, this will be a trait that allows a call back for when this node completes.

        :param node: The node that is waiting for this node to complete.
        """
        node = node.add_numeration(self.node_exist_in_chain(node))
        self.add_subscriber(node)
        return node

    def add_numeration(self, num_matches):
        if num_matches != 0:
            return Pod(self.name + "<{}>".format(num_matches))
        else:
            return self

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
            print(self.name)
            return [self]
        else:
            return [parent.find_root_nodes()[0] for parent in self.parent]


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
