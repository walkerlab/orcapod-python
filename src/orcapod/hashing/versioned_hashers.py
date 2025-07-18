# A collection of versioned hashers that provide a "default" implementation of hashers.
from .arrow_hashers import SemanticArrowHasher
from orcapod.utils.object_spec import parse_objectspec
from orcapod.protocols.hashing_protocols import ObjectHasher
from typing import Any

CURRENT_VERSION = "v0.1"

versioned_semantic_arrow_hashers = {
    "v0.1": {
        "_class": "orcapod.hashing.arrow_hashers.SemanticArrowHasher",
        "_config": {
            "hasher_id": "arrow_v0.1",
            "hash_algorithm": "sha256",
            "chunk_size": 8192,
            "serialization_method": "logical",
            "semantic_type_hashers": {
                "path": {
                    "_class": "orcapod.hashing.semantic_type_hashers.PathHasher",
                    "_config": {
                        "file_hasher": {
                            "_class": "orcapod.hashing.file_hashers.BasicFileHasher",
                            "_config": {
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
        "_config": {
            "hasher_id": "object_v0.1",
            "function_info_extractor": {
                "_class": "orcapod.hashing.function_info_extractors.FunctionSignatureExtractor",
                "_config": {"include_module": True, "include_defaults": True},
            },
        },
    }
}


def get_versioned_semantic_arrow_hasher(
    version: str | None = None,
) -> SemanticArrowHasher:
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
