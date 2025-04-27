from typing import Dict, Any, Union, Optional, List, Tuple, Protocol, Sequence, Mapping
from typing_extensions import TypeAlias
from os import PathLike

# arbitrary depth of nested list of strings or None
L: TypeAlias = Sequence[Union[str, None, "L"]]

# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag = Mapping[str, Union[str, L]]

# a pathset is a path or an arbitrary depth of nested list of paths
Pathset: TypeAlias = Union[PathLike, Sequence["Pathset"]]

# a packet is a mapping from string keys to pathsets
Packet = Mapping[str, Pathset]

# a batch is a tuple of a tag and a list of packets
Batch = Tuple[Tag, Sequence[Packet]]

class PodFunction(Protocol):
    """
    A function suitable to be used in a FunctionPod.
    It takes one or more named arguments, each corresponding to a path to a file or directory,
    and returns a path or a list of paths
    """
    def __call__(self, **kwargs: Pathset) -> Union[PathLike, List[Pathset]]: ...