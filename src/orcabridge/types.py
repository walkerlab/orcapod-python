from typing import Union, List, Tuple, Protocol, Sequence, Mapping
from anyio import Path
from typing_extensions import TypeAlias
from os import PathLike

# arbitrary depth of nested list of strings or None
L: TypeAlias = Sequence[Union[str, None, "L"]]

# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag: TypeAlias = Mapping[str, Union[str, L]]

# a pathset is a path or an arbitrary depth of nested list of paths
Pathset: TypeAlias = Union[PathLike, Sequence[PathLike]]

# a packet is a mapping from string keys to pathsets
Packet: TypeAlias = Mapping[str, Pathset]

# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = Tuple[Tag, Sequence[Packet]]


class PodFunction(Protocol):
    """
    A function suitable to be used in a FunctionPod.
    It takes one or more named arguments, each corresponding to a path to a file or directory,
    and returns a path or a list of paths
    """

    def __call__(self, **kwargs: Pathset) -> Union[PathLike, List[Pathset]]: ...
