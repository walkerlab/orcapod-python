from .model import Pipeline
from .crypto import hash_buffer
from pathlib import Path
from orcapod import LocalFileStore


def _save_pipeline(self, pipeline: Pipeline):

    for node in pipeline._pipeline["metadata"].values():
        if node.__class__.__name__.lower() == "pod":
            self.save_pod(node)

    pipeline_yaml = pipeline._to_yaml()

    hash = hash_buffer(buffer=pipeline_yaml["spec"].encode())

    annotation_dir = Path(f"{self.directory()}/orcapod_model/pipeline/{hash}/annotation")
    annotation_dir.mkdir(parents=True, exist_ok=True)
    with open(annotation_dir.parent / "spec.yaml", "w") as f:
        f.write(pipeline_yaml["spec"])
    if pipeline_yaml["annotation"]:
        with open(
            annotation_dir
            / f'{pipeline._pipeline["annotation"]["name"]}-{pipeline._pipeline["annotation"]["version"]}.yaml',
            "w",
        ) as f:
            f.write(pipeline_yaml["annotation"])


store = LocalFileStore(directory="../.tmp")
store.save_pipeline = _save_pipeline.__get__(store)
