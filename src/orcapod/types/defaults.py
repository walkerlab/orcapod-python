# A collection of versioned hashers that provide a "default" implementation of hashers.
from orcapod.utils.object_spec import parse_objectspec


from orcapod.types.semantic_types import (
    SemanticTypeRegistry,
    SemanticType,
    CanonicalPath,
    PathlibPathConverter,
    ArrowStringPathConverter,
)

CURRENT_VERSION = "v0.1"


semantic_path_objectspec = {
    "v0.1": {
        "_class": "orcapod.types.semantic_types.SemanticType",
        "_config": {
            "name": "path",
            "description": "File system path representation",
            "python_converters": [
                {
                    "_class": "orcapod.types.semantic_types.PathlibPathConverter",
                }
            ],
            "arrow_converters": [
                {
                    "_class": "orcapod.types.semantic_types.ArrowStringPathConverter",
                }
            ],
        },
    }
}

semantic_registry_objectspec = {
    "v0.1": {
        "_class": "orcapod.types.semantic_types.SemanticTypeRegistry",
        "_config": {"semantic_types": [semantic_path_objectspec["v0.1"]]},
    }
}


SEMANTIC_PATH = SemanticType[CanonicalPath](
    "path",
    "File system path representation",
    python_converters=[PathlibPathConverter()],
    arrow_converters=[ArrowStringPathConverter()],
)

DEFAULT_REGISTRY = SemanticTypeRegistry([SEMANTIC_PATH])
