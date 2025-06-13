# src/orcabridge/types.py
import os
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Any, Protocol
from typing_extensions import TypeAlias


SUPPORTED_PYTHON_TYPES = (str, int, float, bool, bytes)

# Convenience alias for anything pathlike
PathLike = str | os.PathLike

# an (optional) string or a collection of (optional) string values
# Note that TagValue can be nested, allowing for an arbitrary depth of nested lists
TagValue: TypeAlias = str | None | Collection["TagValue"]

# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag: TypeAlias = Mapping[str, TagValue]

# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# Simple data types that we support (with clear Polars correspondence)
SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes

ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathLike

# Extended data values that can be stored in packets
# Either the original PathSet or one of our supported simple data types
DataValue: TypeAlias = PathSet | SupportedNativePythonData | Collection["DataValue"]


# a packet is a mapping from string keys to data values
Packet: TypeAlias = Mapping[str, DataValue]

# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[Tag, Collection[Packet]]


class PodFunction(Protocol):
    """
    A function suitable to be used in a FunctionPod.
    It takes one or more named arguments, each corresponding to either:
    - A path to a file or directory (PathSet) - for backward compatibility
    - A simple data value (str, int, float, bool, bytes, Path)
    and returns either None, a single value, or a list of values
    """

    def __call__(self, **kwargs: DataValue) -> None | DataValue | list[DataValue]: ...
