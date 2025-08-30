from types import UnionType
from typing import TypeAlias
import os
from collections.abc import Collection, Mapping

import logging

logger = logging.getLogger(__name__)

DataType: TypeAlias = type | UnionType | list[type] | tuple[type, ...]

PythonSchema: TypeAlias = dict[str, DataType]  # dict of parameter names to their types

PythonSchemaLike: TypeAlias = Mapping[
    str, DataType
]  # Mapping of parameter names to their types

# Convenience alias for anything pathlike
PathLike = str | os.PathLike

# an (optional) string or a collection of (optional) string values
# Note that TagValue can be nested, allowing for an arbitrary depth of nested lists
TagValue: TypeAlias = int | str | None | Collection["TagValue"]

# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# Simple data types that we support (with clear Polars correspondence)
SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes

ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathSet

# Extended data values that can be stored in packets
# Either the original PathSet or one of our supported simple data types
DataValue: TypeAlias = ExtendedSupportedPythonData | Collection["DataValue"] | None

PacketLike: TypeAlias = Mapping[str, DataValue]
