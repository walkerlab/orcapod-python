from typing import Protocol, Any, TypeAlias
import os
from collections.abc import Collection, Mapping

import logging


DataType: TypeAlias = type

TypeSpec: TypeAlias = Mapping[
    str, DataType
]  # Mapping of parameter names to their types

logger = logging.getLogger(__name__)


# class TypeSpec(dict[str, DataType]):
#     def __init__(self, *args, **kwargs):
#         """
#         TypeSpec is a mapping of parameter names to their types.
#         It can be used to define the expected types of parameters in a function or a pod.
#         """
#         super().__init__(*args, **kwargs)


# Convenience alias for anything pathlike
PathLike = str | os.PathLike

# an (optional) string or a collection of (optional) string values
# Note that TagValue can be nested, allowing for an arbitrary depth of nested lists
TagValue: TypeAlias = int | str | None | Collection["TagValue"]

# the top level tag is a mapping from string keys to values that can be a string or
# an arbitrary depth of nested list of strings or None
Tag: TypeAlias = Mapping[str, TagValue]

# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# Simple data types that we support (with clear Polars correspondence)
SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes

ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathSet

# Extended data values that can be stored in packets
# Either the original PathSet or one of our supported simple data types
DataValue: TypeAlias = ExtendedSupportedPythonData | Collection["DataValue"] | None

StoreValue: TypeAlias = SupportedNativePythonData | Collection["StoreValue"] | None

PacketLike: TypeAlias = Mapping[str, DataValue]


# class PodFunction(Protocol):
#     """
#     A function suitable to be used in a FunctionPod.
#     It takes one or more named arguments, each corresponding to either:
#     - A path to a file or directory (PathSet) - for backward compatibility
#     - A simple data value (str, int, float, bool, bytes, Path)
#     and returns either None, a single value, or a list of values
#     """

#     def __call__(self, **kwargs: DataValue) -> None | DataValue | list[DataValue]: ...


class TypeHandler(Protocol):
    """Protocol for handling conversion between Python type and Arrow
    data types used for storage.

    The handler itself IS the definition of a semantic type. The semantic type
    name/identifier is provided by the registerer when registering the handler.

    TypeHandlers should clearly communicate what Python types they can handle,
    and focus purely on conversion logic.
    """

    def python_type(self) -> type:
        """Return the Python type(s) this handler can process.

        Returns:
            Python type the handler supports

        Examples:
            - PathHandler: return Path
            - NumericHandler: return (int, float)
            - CollectionHandler: return (list, tuple, set)
        """
        ...

    def storage_type(self) -> type:
        """Return the Arrow DataType instance for schema definition."""
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert Python value to Arrow-compatible storage representation."""
        ...

    def storage_to_python(self, value: Any) -> Any:
        """Convert storage representation back to Python object."""
        ...
