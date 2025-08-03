from .core import PathLike, PathSet, TypeSpec, DataValue
from . import typespec_utils

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
