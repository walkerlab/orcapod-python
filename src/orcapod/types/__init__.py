from .core import PathLike, PathSet, TypeSpec, DataValue
from . import typespec_utils
from .defaults import DEFAULT_REGISTRY as default_registry

Packet = dict[str, str]
PacketLike = Packet


__all__ = [
    "TypeSpec",
    "PathLike",
    "PathSet",
    "typespec_utils",
    "DataValue",
    "default_registry",
]
