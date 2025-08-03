from typing import Any, TYPE_CHECKING
from collections.abc import Collection
from orcapod.protocols.semantic_protocols import SemanticStructConverter
from orcapod.utils.lazy_module import LazyModule


if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class SemanticTypeRegistry:
    """
    Registry that manages semantic type converters using struct signature recognition.

    This registry maps Python types to PyArrow struct signatures, enabling
    automatic detection and conversion of semantic types based on their
    struct schema alone.
    """

    def __init__(self, converters: Collection[SemanticStructConverter] | None = None):
        # Bidirectional mappings between Python types and struct signatures
        self._python_to_struct: dict[type, "pa.StructType"] = {}
        self._struct_to_python: dict["pa.StructType", type] = {}
        self._struct_to_converter: dict["pa.StructType", SemanticStructConverter] = {}

        # Name mapping for convenience
        self._name_to_converter: dict[str, SemanticStructConverter] = {}
        self._struct_to_name: dict["pa.StructType", str] = {}

        if converters:
            for converter in converters:
                self.register_converter(converter)

    def register_converter(
        self, converter: SemanticStructConverter, semantic_name: str | None = None
    ) -> None:
        """
        Register a semantic type converter.

        This creates bidirectional mappings between:
        - Python type ↔ Arrow struct signature
        - Arrow struct signature ↔ converter instance

        Optionally, a semantic type name can be provided.
        """
        python_type = converter.python_type
        struct_signature = converter.arrow_struct_type

        # Check for conflicts
        if python_type in self._python_to_struct:
            existing_struct = self._python_to_struct[python_type]
            if existing_struct != struct_signature:
                raise ValueError(
                    f"Python type {python_type} already registered with different struct signature. "
                    f"Existing: {existing_struct}, New: {struct_signature}"
                )

        if struct_signature in self._struct_to_python:
            existing_python = self._struct_to_python[struct_signature]
            if existing_python != python_type:
                raise ValueError(
                    f"Struct signature {struct_signature} already registered with different Python type. "
                    f"Existing: {existing_python}, New: {python_type}"
                )

        if semantic_name in self._name_to_converter:
            existing = self._name_to_converter[semantic_name]
            if existing != converter:
                raise ValueError(
                    f"Semantic type name '{semantic_name}' already registered"
                )

        # Register bidirectional mappings
        self._python_to_struct[python_type] = struct_signature
        self._struct_to_python[struct_signature] = python_type
        self._struct_to_converter[struct_signature] = converter
        if semantic_name is not None:
            self._name_to_converter[semantic_name] = converter
            self._struct_to_name[struct_signature] = semantic_name

    def get_converter_for_python_type(
        self, python_type: type
    ) -> SemanticStructConverter | None:
        """Get converter for a Python type."""
        # Direct lookup first
        struct_signature = self._python_to_struct.get(python_type)
        if struct_signature:
            return self._struct_to_converter[struct_signature]

        # Handle subclass relationships - add safety check
        for registered_type, struct_signature in self._python_to_struct.items():
            try:
                if (
                    isinstance(registered_type, type)
                    and isinstance(python_type, type)
                    and issubclass(python_type, registered_type)
                ):
                    return self._struct_to_converter[struct_signature]
            except TypeError:
                # Handle cases where issubclass fails (e.g., with generic types)
                continue

        return None

    def get_converter_for_semantic_type(
        self, semantic_type_name: str
    ) -> SemanticStructConverter | None:
        """Get converter by semantic type name."""
        return self._name_to_converter.get(semantic_type_name)

    def get_converter_for_struct_signature(
        self, struct_signature: "pa.StructType"
    ) -> SemanticStructConverter | None:
        """
        Get converter for an Arrow struct signature.

        This is the core method for struct signature recognition.
        """
        return self._struct_to_converter.get(struct_signature)

    def get_python_type_for_semantic_struct_signature(
        self, struct_signature: "pa.StructType"
    ) -> type | None:
        """
        Get Python type for an Arrow struct signature.

        This enables automatic type inference from struct schemas.
        """
        return self._struct_to_python.get(struct_signature)

    def get_semantic_struct_signature_for_python_type(
        self, python_type: type
    ) -> "pa.StructType | None":
        """Get Arrow struct signature for a Python type."""
        return self._python_to_struct.get(python_type)

    def is_semantic_struct_signature(self, struct_signature: "pa.StructType") -> bool:
        """Check if a struct signature represents a semantic type."""
        return struct_signature in self._struct_to_python

    def has_python_type(self, python_type: type) -> bool:
        """Check if a Python type is registered."""
        return python_type in self._python_to_struct

    def has_semantic_type(self, semantic_type_name: str) -> bool:
        """Check if a semantic type name is registered."""
        return semantic_type_name in self._name_to_converter

    def list_semantic_types(self) -> list[str]:
        """Get all registered semantic type names."""
        return list(self._name_to_converter.keys())

    def list_python_types(self) -> list[type]:
        """Get all registered Python types."""
        return list(self._python_to_struct.keys())

    def list_struct_signatures(self) -> list["pa.StructType"]:
        """Get all registered struct signatures."""
        return list(self._struct_to_python.keys())

    def find_semantic_fields_in_schema(self, schema: "pa.Schema") -> dict[str, str]:
        """
        Find all semantic type fields in a schema by struct signature recognition.

        Args:
            schema: PyArrow schema to examine

        Returns:
            Dictionary mapping field names to semantic type names

        Example:
            schema with fields:
            - name: string
            - file_path: struct<path: large_string>
            - location: struct<latitude: double, longitude: double>

            Returns: {"file_path": "path", "location": "geolocation"}
        """
        semantic_fields = {}
        for field in schema:
            if pa.types.is_struct(field.type) and field.type in self._struct_to_name:
                semantic_fields[field.name] = self._struct_to_name[field.type]
        return semantic_fields

    def get_semantic_field_info(self, schema: "pa.Schema") -> dict[str, dict[str, Any]]:
        """
        Get detailed information about semantic fields in a schema.

        Returns:
            Dictionary with field names as keys and info dictionaries as values.
            Each info dict contains: semantic_type, python_type, struct_signature
        """
        semantic_info = {}
        for field in schema:
            if pa.types.is_struct(field.type):
                converter = self.get_converter_for_struct_signature(field.type)
                if converter:
                    semantic_info[field.name] = {
                        "python_type": converter.python_type,
                        "struct_signature": field.type,
                        "converter": converter,
                    }
        return semantic_info

    def validate_struct_signature(
        self, struct_signature: "pa.StructType", expected_python_type: type
    ) -> bool:
        """
        Validate that a struct signature matches the expected Python type.

        Args:
            struct_signature: Arrow struct type to validate
            expected_python_type: Expected Python type

        Returns:
            True if the struct signature is registered for the Python type
        """
        registered_type = self.get_python_type_for_semantic_struct_signature(
            struct_signature
        )
        return registered_type == expected_python_type


# # Conversion utilities using struct signature recognition
# class SemanticStructConverter:
#     """Main converter class for working with semantic structs using signature recognition."""

#     def __init__(self, registry: SemanticTypeRegistry):
#         self.registry = registry

#     def python_to_struct_dict(self, value: Any) -> dict[str, Any] | None:
#         """Convert Python value to struct dict if it's a semantic type."""
#         converter = self.registry.get_converter_for_python_type(type(value))
#         if converter:
#             return converter.python_to_struct_dict(value)
#         return None

#     def struct_dict_to_python(
#         self, struct_dict: dict[str, Any], struct_signature: "pa.StructType"
#     ) -> Any:
#         """
#         Convert struct dict back to Python value using struct signature recognition.

#         Args:
#             struct_dict: Dictionary representation of the struct
#             struct_signature: PyArrow struct type signature

#         Returns:
#             Python object corresponding to the semantic type
#         """
#         converter = self.registry.get_converter_for_struct_signature(struct_signature)
#         if not converter:
#             raise ValueError(
#                 f"No converter found for struct signature: {struct_signature}"
#             )

#         return converter.struct_dict_to_python(struct_dict)

#     def is_semantic_struct_dict(
#         self, struct_dict: dict[str, Any], struct_signature: "pa.StructType"
#     ) -> bool:
#         # FIXME: inconsistent implementation -- should check the passed in struct_dict
#         """Check if a dict represents a semantic struct based on signature."""
#         return self.registry.is_semantic_struct_signature(struct_signature)

#     def get_semantic_type_from_struct_signature(
#         self, struct_signature: "pa.StructType"
#     ) -> str | None:
#         """Extract semantic type name from struct signature."""
#         converter = self.registry.get_converter_for_struct_signature(struct_signature)
#         return converter.semantic_type_name if converter else None

#     def python_to_arrow_array(self, values: list[Any]) -> "pa.Array":
#         """Convert list of Python values to Arrow array of structs."""
#         if not values:
#             raise ValueError("Cannot convert empty list")

#         # Check if first value is a semantic type
#         first_converter = self.registry.get_converter_for_python_type(type(values[0]))
#         if not first_converter:
#             raise ValueError(f"No semantic type converter for {type(values[0])}")

#         # Convert all values to struct dicts
#         struct_dicts = []
#         for value in values:
#             converter = self.registry.get_converter_for_python_type(type(value))
#             if converter is None or converter != first_converter:
#                 raise ValueError("All values must be the same semantic type")
#             struct_dicts.append(converter.python_to_struct_dict(value))

#         # Create Arrow array with the registered struct signature
#         return pa.array(struct_dicts, type=first_converter.arrow_struct_type)

#         # Create Arrow array with the registered struct signature
#         return pa.array(struct_dicts, type=first_converter.arrow_struct_type)

#     def arrow_array_to_python(self, array: "pa.Array") -> list[Any]:
#         """Convert Arrow struct array back to list of Python values."""
#         if not pa.types.is_struct(array.type):
#             raise ValueError(f"Expected struct array, got {array.type}")

#         converter = self.registry.get_converter_for_struct_signature(array.type)
#         if not converter:
#             raise ValueError(f"No converter found for struct signature: {array.type}")

#         # Convert each struct to Python value
#         python_values = []
#         for i in range(len(array)):
#             struct_scalar = array[i]
#             if struct_scalar.is_valid:
#                 struct_dict = struct_scalar.as_py()
#                 python_values.append(converter.struct_dict_to_python(struct_dict))
#             else:
#                 python_values.append(None)

#         return python_values
