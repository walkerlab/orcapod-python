from typing import Protocol
from collections.abc import Collection, Mapping
from typing_extensions import TypeAlias
import os

# Convenience alias for anything pathlike
PathLike = str | os.PathLike

# an (optional) string or a collection of (optional) string values
TagValue: TypeAlias = str | None | Collection[str | None]


# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag: TypeAlias = Mapping[str, TagValue]


# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# a packet is a mapping from string keys to pathsets
Packet: TypeAlias = Mapping[str, PathSet]

# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[Tag, Collection[Packet]]


class PodFunction(Protocol):
    """
    A function suitable to be used in a FunctionPod.
    It takes one or more named arguments, each corresponding to a path to a file or directory,
    and returns a path or a list of paths
    """

    def __call__(self, **kwargs: PathSet) -> None | PathSet | list[PathSet]: ...
