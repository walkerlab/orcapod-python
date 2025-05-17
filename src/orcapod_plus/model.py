from .util import yaml
import pygraphviz as pgv
from IPython.display import Image
from orcapod import ModelId, LocalFileStore, Pod, Annotation, StreamInfo
from networkx.drawing import nx_agraph
import networkx as nx


class Pipeline:
    @classmethod
    def from_yaml(cls, pipeline_input: str, store: LocalFileStore = None) -> dict:
        pipeline = yaml.safe_load(pipeline_input)
        # are there links w/o providing a store?
        if "link" in pipeline_input and not store:
            raise Exception("Store needed to resolve links.")
        # add validation checks e.g. do links exist? is syntax correct?
        pass
        # resolve name resolution (hash->hash + latest annotation?, annotation->hash+annotation)
        pass
        # save inline definitions
        pass
        # normalize the node names for consistent hashing
        pass
        # add style convention
        shape_lookup = {
            "pod": "box",
            "mapper": "circle",
            "curator": "triangle",
            "input": "star",
        }
        title = f'label="{pipeline["annotation"]["name"]}"' if pipeline["annotation"] else ""
        nodes = []
        for name, info in pipeline["metadata"].items():
            if info["spec"]["class"] == "pod" and "link" in info["spec"] and "hash" in info["spec"]["link"]:
                pipeline["metadata"][name] = store.load_pod(model_id=ModelId.HASH(info["spec"]["link"]["hash"]))
            elif info["spec"]["class"] == "pod" and "link" not in info["spec"]:
                pipeline["metadata"][name] = Pod(
                    annotation=(Annotation(**info["annotation"]) if info["annotation"] else None),
                    **{
                        k: v
                        for k, v in info["spec"].items()
                        if k
                        in (
                            "image",
                            "command",
                            "output_dir",
                            "source_commit_url",
                            "recommended_cpus",
                            "recommended_memory",
                        )
                    },
                    input_stream={k: StreamInfo(**v) for k, v in info["spec"]["input_stream"].items()},
                    output_stream={k: StreamInfo(**v) for k, v in info["spec"]["output_stream"].items()},
                    required_gpu=None,  # skipped "required_gpu" for now
                )
            nodes.append(
                f'{name} [shape="{shape_lookup[info["spec"]["class"]]}", label="{name}\n({pipeline["metadata"][name].image()})"]'
            )
        nodes = "\n".join(nodes)

        pipeline["style"] = f"{title}\n{nodes}"

        instance = cls()
        instance._pipeline = pipeline

        return instance

    def _to_yaml(self) -> dict:
        pipeline_spec = self._pipeline.copy()
        pipeline_annotation = pipeline_spec.pop("annotation")

        pipeline_spec["metadata"] = self._pipeline["metadata"].copy()
        for name, info in pipeline_spec["metadata"].items():
            pipeline_spec["metadata"][name] = {
                "spec": {
                    "class": info.__class__.__name__.lower(),
                    "link": (
                        {"name": info.annotation().name, "version": info.annotation().version}
                        if info.annotation()
                        else {"hash": info.hash()}
                    ),
                }
            }

        return {
            "spec": yaml.dump(pipeline_spec),
            "annotation": yaml.dump(pipeline_annotation) if pipeline_annotation else None,
        }

    def _to_dot(self, with_style: bool = True) -> str:
        return pgv.AGraph(
            f"""
            digraph {{
                {self._pipeline["definition"]}
                {self._pipeline["style"] if with_style else ""}
            }}
            """
        )

    def draw_dot(self, with_style: bool = True) -> Image:
        return Image(data=self._to_dot(with_style).draw(format="png", prog="dot", path=None))

    def _to_graph(self, with_style: bool = True):
        return nx_agraph.from_agraph(self._to_dot(with_style))

    def draw_networkx(self, with_style: bool = True):
        return nx.draw(
            (graph := self._to_graph(with_style)),
            pos=nx_agraph.graphviz_layout(graph, prog="dot"),
            with_labels=True,
            node_color="orange",
            node_size=400,
            edge_color="black",
            linewidths=1,
            font_size=15,
        )
