from typing import Protocol, Any, TypeAlias
import pyarrow as pa
from dataclasses import dataclass
import os
from collections.abc import Collection, Mapping


# TODO: reconsider the need for this dataclass as its information is superfluous
# to the registration of the handler into the registry.
@dataclass
class TypeInfo:
    python_type: type
    arrow_type: pa.DataType
    semantic_type: str | None  # name under which the type is registered
    handler: "TypeHandler"


DataType: TypeAlias = type

TypeSpec: TypeAlias = Mapping[
    str, DataType
]  # Mapping of parameter names to their types


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


class TypeHandler(Protocol):
    """Protocol for handling conversion between Python types and underlying Arrow
    data types used for storage.

    The handler itself IS the definition of a semantic type. The semantic type
    name/identifier is provided by the registerer when registering the handler.

    TypeHandlers should clearly communicate what Python types they can handle,
    and focus purely on conversion logic.
    """

    def python_types(self) -> type | tuple[type, ...]:
        """Return the Python type(s) this handler can process.

        Returns:
            Single Type or tuple of Types this handler supports

        Examples:
            - PathHandler: return Path
            - NumericHandler: return (int, float)
            - CollectionHandler: return (list, tuple, set)
        """
        ...

    def storage_type(self) -> pa.DataType:
        """Return the Arrow DataType instance for schema definition."""
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert Python value to Arrow-compatible storage representation."""
        ...

    def storage_to_python(self, value: Any) -> Any:
        """Convert storage representation back to Python object."""
        ...
