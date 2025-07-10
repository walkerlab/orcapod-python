from typing import Protocol, Any


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
