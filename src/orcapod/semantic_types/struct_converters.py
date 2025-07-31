"""
Struct-based semantic type system for OrcaPod.

This replaces the metadata-based approach with explicit struct fields,
making semantic types visible in schemas and preserved through operations.
"""

from typing import Any, Protocol
from pathlib import Path
import pyarrow as pa
from collections.abc import Collection


# Core protocols
class StructConverter(Protocol):
    """Protocol for converting between Python objects and semantic structs."""

    @property
    def semantic_type_name(self) -> str:
        """The semantic type name this converter handles."""
        ...

    @property
    def python_type(self) -> type:
        """The Python type this converter can handle."""
        ...

    @property
    def arrow_struct_type(self) -> pa.StructType:
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

    def can_handle_struct_type(self, struct_type: pa.StructType) -> bool:
        """Check if this converter can handle the given struct type."""
        ...


# Path-specific implementation
class PathStructConverter:
    """Converter for pathlib.Path objects to/from semantic structs."""

    def __init__(self):
        self._semantic_type_name = "path"
        self._python_type = Path

        # Define the Arrow struct type for paths
        self._arrow_struct_type = pa.struct(
            [
                pa.field("semantic_type", pa.string()),
                pa.field("path", pa.large_string()),
            ]
        )

    @property
    def semantic_type_name(self) -> str:
        return self._semantic_type_name

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
            "semantic_type": self._semantic_type_name,
            "path": str(value),
        }

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Path:
        """Convert struct dictionary back to Path."""
        if struct_dict.get("semantic_type") != self._semantic_type_name:
            raise ValueError(
                f"Expected semantic_type '{self._semantic_type_name}', "
                f"got '{struct_dict.get('semantic_type')}'"
            )

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
        expected_fields = {"semantic_type", "path"}

        if set(field_names) != expected_fields:
            return False

        # Check field types
        field_types = {field.name: field.type for field in struct_type}

        return (
            field_types["semantic_type"] == pa.string()
            and field_types["path"] == pa.large_string()
        )

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a struct dictionary represents this semantic type."""
        return struct_dict.get("semantic_type") == self._semantic_type_name


# Registry for managing semantic type converters
class SemanticTypeRegistry:
    """Registry that manages struct-based semantic type converters."""

    def __init__(self, converters: Collection[StructConverter] | None = None):
        self._python_to_converter: dict[type, StructConverter] = {}
        self._name_to_converter: dict[str, StructConverter] = {}
        self._struct_type_to_converter: dict[pa.StructType, StructConverter] = {}

        if converters:
            for converter in converters:
                self.register_converter(converter)

    def register_converter(self, converter: StructConverter) -> None:
        """Register a semantic type converter."""
        # Register by Python type
        python_type = converter.python_type
        if python_type in self._python_to_converter:
            existing = self._python_to_converter[python_type]
            raise ValueError(
                f"Python type {python_type} already registered with converter "
                f"for semantic type '{existing.semantic_type_name}'"
            )
        self._python_to_converter[python_type] = converter

        # Register by semantic type name
        name = converter.semantic_type_name
        if name in self._name_to_converter:
            raise ValueError(f"Semantic type '{name}' already registered")
        self._name_to_converter[name] = converter

        # Register by struct type
        struct_type = converter.arrow_struct_type
        self._struct_type_to_converter[struct_type] = converter

    def get_converter_for_python_type(
        self, python_type: type
    ) -> StructConverter | None:
        """Get converter for a Python type."""
        # Direct lookup first
        converter = self._python_to_converter.get(python_type)
        if converter:
            return converter

        # Check for subclass relationships
        for registered_type, converter in self._python_to_converter.items():
            if issubclass(python_type, registered_type):
                return converter

        return None

    def get_converter_for_semantic_type(
        self, semantic_type_name: str
    ) -> StructConverter | None:
        """Get converter by semantic type name."""
        return self._name_to_converter.get(semantic_type_name)

    def get_converter_for_struct_type(
        self, struct_type: pa.StructType
    ) -> StructConverter | None:
        """Get converter for an Arrow struct type."""
        # Direct lookup first
        converter = self._struct_type_to_converter.get(struct_type)
        if converter:
            return converter

        # Check if any converter can handle this struct type
        for converter in self._name_to_converter.values():
            if converter.can_handle_struct_type(struct_type):
                return converter

        return None

    def is_semantic_struct_type(self, struct_type: pa.StructType) -> bool:
        """Check if a struct type represents a semantic type."""
        return self.get_converter_for_struct_type(struct_type) is not None

    def has_python_type(self, python_type: type) -> bool:
        """Check if a Python type is registered."""
        return self.get_converter_for_python_type(python_type) is not None

    def has_semantic_type(self, semantic_type_name: str) -> bool:
        """Check if a semantic type is registered."""
        return semantic_type_name in self._name_to_converter

    def list_semantic_types(self) -> list[str]:
        """Get all registered semantic type names."""
        return list(self._name_to_converter.keys())

    def list_python_types(self) -> list[type]:
        """Get all registered Python types."""
        return list(self._python_to_converter.keys())


# Conversion utilities
class SemanticStructConverter:
    """Main converter class for working with semantic structs."""

    def __init__(self, registry: SemanticTypeRegistry):
        self.registry = registry

    def python_to_struct_dict(self, value: Any) -> dict[str, Any] | None:
        """Convert Python value to struct dict if it's a semantic type."""
        converter = self.registry.get_converter_for_python_type(type(value))
        if converter:
            return converter.python_to_struct_dict(value)
        return None

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        """Convert struct dict back to Python value."""
        semantic_type = struct_dict.get("semantic_type")
        if not semantic_type:
            raise ValueError("Struct dict missing 'semantic_type' field")

        converter = self.registry.get_converter_for_semantic_type(semantic_type)
        if not converter:
            raise ValueError(f"No converter found for semantic type '{semantic_type}'")

        return converter.struct_dict_to_python(struct_dict)

    def is_semantic_struct_dict(self, struct_dict: dict[str, Any]) -> bool:
        """Check if a dict represents a semantic struct."""
        semantic_type = struct_dict.get("semantic_type")
        if not semantic_type:
            return False
        return self.registry.has_semantic_type(semantic_type)

    def python_to_arrow_array(self, values: list[Any]) -> pa.Array:
        """Convert list of Python values to Arrow array of structs."""
        if not values:
            raise ValueError("Cannot convert empty list")

        # Check if first value is a semantic type
        first_converter = self.registry.get_converter_for_python_type(type(values[0]))
        if not first_converter:
            raise ValueError(f"No semantic type converter for {type(values[0])}")

        # Convert all values to struct dicts
        struct_dicts = []
        for value in values:
            converter = self.registry.get_converter_for_python_type(type(value))
            if converter is None or converter != first_converter:
                raise ValueError("All values must be the same semantic type")
            struct_dicts.append(converter.python_to_struct_dict(value))

        # Create Arrow array
        return pa.array(struct_dicts, type=first_converter.arrow_struct_type)

    def arrow_array_to_python(self, array: pa.Array) -> list[Any]:
        """Convert Arrow struct array back to list of Python values."""
        if not pa.types.is_struct(array.type):
            raise ValueError(f"Expected struct array, got {array.type}")

        converter = self.registry.get_converter_for_struct_type(array.type)
        if not converter:
            raise ValueError(f"No converter found for struct type {array.type}")

        # Convert each struct to Python value
        python_values = []
        for i in range(len(array)):
            struct_scalar = array[i]
            if struct_scalar.is_valid:
                struct_dict = struct_scalar.as_py()
                python_values.append(converter.struct_dict_to_python(struct_dict))
            else:
                python_values.append(None)

        return python_values


# Default registry with Path support
def create_default_registry() -> SemanticTypeRegistry:
    """Create default registry with built-in semantic types."""
    registry = SemanticTypeRegistry()
    registry.register_converter(PathStructConverter())
    return registry


# Global default registry
DEFAULT_REGISTRY = create_default_registry()
