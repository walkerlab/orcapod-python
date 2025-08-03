"""
Struct-based semantic type system for OrcaPod.

This replaces the metadata-based approach with explicit struct fields,
making semantic types visible in schemas and preserved through operations.
"""

from typing import Any, TYPE_CHECKING
from pathlib import Path
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


# Path-specific implementation
class PathStructConverter:
    """Converter for pathlib.Path objects to/from semantic structs."""

    def __init__(self):
        self._python_type = Path

        # Define the Arrow struct type for paths
        self._arrow_struct_type = pa.struct(
            [
                pa.field("path", pa.large_string()),
            ]
        )

    @property
    def python_type(self) -> type:
        return self._python_type

    @property
    def arrow_struct_type(self) -> pa.StructType:
        return self._arrow_struct_type

    def python_to_struct_dict(self, value: Path) -> dict[str, Any]:
        """Convert Path to struct dictionary."""
        if not isinstance(value, Path):
            raise TypeError(f"Expected Path, got {type(value)}")

        return {
            "path": str(value),
        }

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Path:
        """Convert struct dictionary back to Path."""
        path_str = struct_dict.get("path")
        if path_str is None:
            raise ValueError("Missing 'path' field in struct")

        return Path(path_str)

    def can_handle_python_type(self, python_type: type) -> bool:
        """Check if this converter can handle the given Python type."""
        return issubclass(python_type, Path)

    def can_handle_struct_type(self, struct_type: pa.StructType) -> bool:
        """Check if this converter can handle the given struct type."""
        # Check if struct has the expected fields
        field_names = [field.name for field in struct_type]
        expected_fields = {"path"}

        if set(field_names) != expected_fields:
            return False

        # Check field types
        field_types = {field.name: field.type for field in struct_type}

        return field_types["path"] == pa.large_string()

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        return set(struct_dict.keys()) == {"path"} and isinstance(
            struct_dict["path"], str
        )
