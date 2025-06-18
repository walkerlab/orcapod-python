from typing import Protocol, Any, TypeAlias, Mapping
import pyarrow as pa
from dataclasses import dataclass


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
