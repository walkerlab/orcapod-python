# A collection of versioned hashers that provide a "default" implementation of hashers.
from .arrow_hashers import SemanticArrowHasher
from .types import ObjectHasher, ArrowHasher
import importlib
from typing import Any

CURRENT_VERSION = "v0.1"

versioned_semantic_arrow_hashers = {
    "v0.1": {
        "_class": "orcapod.hashing.arrow_hashers.SemanticArrowHasher",
        "config": {
            "hasher_id": "arrow_v0.1",
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

versioned_object_hashers = {
    "v0.1": {
        "_class": "orcapod.hashing.object_hashers.BasicObjectHasher",
        "config": {
            "hasher_id": "object_v0.1",
            "function_info_extractor" : {
                "_class": "orcapod.hashing.function_info_extractors.FunctionSignatureExtractor",
                "config": {
                    "include_module": True,
                    "include_defaults": True
                }

            }
        }
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
) -> ArrowHasher:
    """
    Get the versioned hasher for the specified version.

    Args:
        version (str): The version of the hasher to retrieve.

    Returns:
        ArrowHasher: An instance of the arrow hasher of the specified version.
    """
    if version is None:
        version = CURRENT_VERSION

    if version not in versioned_semantic_arrow_hashers:
        raise ValueError(f"Unsupported hasher version: {version}")

    hasher_spec = versioned_semantic_arrow_hashers[version]
    return parse_objectspec(hasher_spec)


def get_versioned_object_hasher(
    version: str | None = None,
) -> ObjectHasher: 
    """
    Get an object hasher for the specified version.

    Args:
        version (str): The version of the hasher to retrieve.

    Returns:
        Object: An instance of the object hasher of the specified version.
    """
    if version is None:
        version = CURRENT_VERSION

    if version not in versioned_object_hashers:
        raise ValueError(f"Unsupported hasher version: {version}")

    hasher_spec = versioned_object_hashers[version]
    return parse_objectspec(hasher_spec)

