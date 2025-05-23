from typing import Union, Tuple, Protocol, Mapping, Collection, Optional
from pathlib import Path
from typing_extensions import TypeAlias
import os

# Convenience alias for anything pathlike
PathLike = Union[str, bytes, os.PathLike]

# arbitrary depth of nested list of strings or None
L: TypeAlias = Union[str, None, Collection[Optional[str]]]


# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag: TypeAlias = Mapping[str, Union[str, L]]


# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = Union[PathLike, Collection[Optional[PathLike]]]

# a packet is a mapping from string keys to pathsets
Packet: TypeAlias = Mapping[str, PathSet]

# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = Tuple[Tag, Collection[Packet]]


class PodFunction(Protocol):
    """
    A function suitable to be used in a FunctionPod.
    It takes one or more named arguments, each corresponding to a path to a file or directory,
    and returns a path or a list of paths
    """

    def __call__(self, **kwargs: PathSet) -> PathSet: ...
