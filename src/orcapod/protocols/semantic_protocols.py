from typing import Protocol, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


# Core protocols
class SemanticStructConverter(Protocol):
    """Protocol for converting between Python objects and semantic structs."""

    @property
    def python_type(self) -> type:
        """The Python type this converter can handle."""
        ...

    @property
    def arrow_struct_type(self) -> "pa.StructType":
        """The Arrow struct type this converter produces."""
        ...

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        """Convert Python value to struct dictionary."""
        ...

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        """Convert struct dictionary back to Python value."""
        ...

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        ...

    def can_handle_struct_type(self, struct_type: "pa.StructType") -> bool:
        """Check if this converter can handle the given struct type."""
        ...
