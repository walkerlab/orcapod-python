# A collection of versioned hashers that provide a "default" implementation of hashers.
from .arrow_hashers import SemanticArrowHasher
import importlib
from typing import Any

CURRENT_VERSION = "v0.1"

versioned_hashers = {
    "v0.1": {
        "_class": "orcapod.hashing.arrow_hashers.SemanticArrowHasher",
        "config": {
            "hasher_id": "default_v0.1",
            "hash_algorithm": "sha256",
            "chunk_size": 8192,
            "semantic_type_hashers": {
                "path": {
                    "_class": "orcapod.hashing.semantic_type_hashers.PathHasher",
                    "config": {
                        "file_hasher": {
                            "_class": "orcapod.hashing.file_hashers.BasicFileHasher",
                            "config": {
                                "algorithm": "sha256",
                            },
                        }
                    },
                }
            },
        },
    }
}


def parse_objectspec(obj_spec: dict) -> Any:
    if "_class" in obj_spec:
        # if _class is specified, treat the dict as an object specification
        module_name, class_name = obj_spec["_class"].rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        configs = parse_objectspec(obj_spec.get("config", {}))
        return cls(**configs)
    else:
        # otherwise, parse through the dictionary recursively
        parsed_object = obj_spec
        for k, v in obj_spec.items():
            if isinstance(v, dict):
                parsed_object[k] = parse_objectspec(v)
            else:
                parsed_object[k] = v
        return parsed_object


def get_versioned_semantic_arrow_hasher(
    version: str | None = None,
) -> SemanticArrowHasher:
    """
    Get the versioned hasher for the specified version.

    Args:
        version (str): The version of the hasher to retrieve.

    Returns:
        SemanticArrowHasher: An instance of the hasher for the specified version.
    """
    if version is None:
        version = CURRENT_VERSION

    if version not in versioned_hashers:
        raise ValueError(f"Unsupported hasher version: {version}")

    hasher_spec = versioned_hashers[version]
    return parse_objectspec(hasher_spec)
