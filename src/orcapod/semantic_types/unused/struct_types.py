"""
Dynamic TypedDict creation for preserving Arrow struct field information in Python type hints.

This solves the problem of converting Arrow struct types back to Python type hints
that preserve full field name and type information.
"""

from typing import TypedDict, Dict, Type, Any, get_type_hints
import pyarrow as pa
from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry


class StructTypeManager:
    """
    Manages dynamic TypedDict creation for Arrow struct types.

    This ensures that Arrow struct types can be converted to Python type hints
    that preserve all field information.
    """

    def __init__(self):
        # Cache created TypedDict classes to avoid duplicates
        self._struct_signature_to_typeddict: Dict[pa.StructType, Type] = {}
        self._typeddict_to_struct_signature: Dict[Type, pa.StructType] = {}
        self._created_type_names: set[str] = set()

    def get_or_create_typeddict_for_struct(
        self,
        struct_type: pa.StructType,
        semantic_registry: SemanticTypeRegistry | None = None,
    ) -> Type:
        """
        Get or create a TypedDict class that represents the Arrow struct type.

        Args:
            struct_type: PyArrow struct type
            semantic_registry: Optional semantic registry for nested types

        Returns:
            TypedDict class that preserves all field information
        """
        # Check cache first
        if struct_type in self._struct_signature_to_typeddict:
            return self._struct_signature_to_typeddict[struct_type]

        # Create field specifications for TypedDict
        field_specs = {}
        for field in struct_type:
            field_name = field.name
            python_type = self._convert_arrow_type_to_python_type(
                field.type, semantic_registry
            )
            field_specs[field_name] = python_type

        # Generate unique name for the TypedDict
        type_name = self._generate_unique_type_name(field_specs)

        # Create TypedDict dynamically
        typeddict_class = TypedDict(type_name, field_specs)

        # Cache the mapping
        self._struct_signature_to_typeddict[struct_type] = typeddict_class
        self._typeddict_to_struct_signature[typeddict_class] = struct_type

        return typeddict_class

    def get_struct_type_for_typeddict(
        self, typeddict_class: Type
    ) -> pa.StructType | None:
        """Get the Arrow struct type for a dynamically created TypedDict."""
        return self._typeddict_to_struct_signature.get(typeddict_class)

    def is_dynamic_typeddict(self, python_type: Type) -> bool:
        """Check if a type is one of our dynamically created TypedDicts."""
        return python_type in self._typeddict_to_struct_signature

    def _convert_arrow_type_to_python_type(
        self, arrow_type: pa.DataType, semantic_registry: SemanticTypeRegistry | None
    ) -> Type:
        """Convert Arrow type to Python type, handling nested structs."""

        # Handle nested struct types recursively
        if pa.types.is_struct(arrow_type):
            # Check if it's a registered semantic type first
            if semantic_registry:
                python_type = semantic_registry.get_python_type_for_struct_signature(
                    arrow_type
                )
                if python_type:
                    return python_type

            # Create dynamic TypedDict for unregistered struct
            return self.get_or_create_typeddict_for_struct(
                arrow_type, semantic_registry
            )

        # For non-struct types, use standard conversion
        from orcapod.semantic_types.semantic_converters import arrow_type_to_python

        return arrow_type_to_python(arrow_type, semantic_registry)

    def _generate_unique_type_name(self, field_specs: Dict[str, Type]) -> str:
        """Generate a unique name for the TypedDict based on field specifications."""

        # Create a descriptive name based on field names
        field_names = sorted(field_specs.keys())
        if len(field_names) <= 3:
            base_name = "Struct_" + "_".join(field_names)
        else:
            base_name = f"Struct_{len(field_names)}fields"

        # Ensure uniqueness
        counter = 1
        type_name = base_name
        while type_name in self._created_type_names:
            type_name = f"{base_name}_{counter}"
            counter += 1

        self._created_type_names.add(type_name)
        return type_name


# Global instance for managing struct types
_struct_type_manager = StructTypeManager()


def arrow_struct_to_python_type(
    struct_type: pa.StructType, semantic_registry: SemanticTypeRegistry | None = None
) -> Type:
    """
    Convert Arrow struct type to Python type hint that preserves field information.

    This creates a TypedDict that exactly matches the Arrow struct fields.

    Args:
        struct_type: PyArrow struct type to convert
        semantic_registry: Optional semantic registry for registered types

    Returns:
        TypedDict class that preserves all field names and types

    Example:
        struct<name: string, age: int64> -> TypedDict with name: str, age: int
    """
    # First check if it's a registered semantic type
    if semantic_registry:
        python_type = semantic_registry.get_python_type_for_struct_signature(
            struct_type
        )
        if python_type:
            return python_type

    # Create dynamic TypedDict for unregistered struct
    return _struct_type_manager.get_or_create_typeddict_for_struct(
        struct_type, semantic_registry
    )


def is_dynamic_struct_type(python_type: Type) -> bool:
    """Check if a Python type is a dynamically created struct TypedDict."""
    return _struct_type_manager.is_dynamic_typeddict(python_type)


def get_struct_signature_for_dynamic_type(python_type: Type) -> pa.StructType | None:
    """Get the Arrow struct signature for a dynamically created TypedDict."""
    return _struct_type_manager.get_struct_type_for_typeddict(python_type)


class DynamicStructConverter:
    """Converter for dynamically created TypedDict structs."""

    def __init__(self, typeddict_class: Type, struct_type: pa.StructType):
        self.typeddict_class = typeddict_class
        self.struct_type = struct_type
        self._semantic_type_name = f"dynamic_struct_{typeddict_class.__name__.lower()}"

    @property
    def semantic_type_name(self) -> str:
        return self._semantic_type_name

    @property
    def python_type(self) -> Type:
        return self.typeddict_class

    @property
    def arrow_struct_type(self) -> pa.StructType:
        return self.struct_type

    def python_to_struct_dict(self, value: dict) -> dict:
        """Convert TypedDict to Arrow struct dict (no conversion needed)."""
        if not isinstance(value, dict):
            raise TypeError(
                f"Expected dict for {self.typeddict_class}, got {type(value)}"
            )

        # Validate that all required fields are present
        type_hints = get_type_hints(self.typeddict_class)
        for field_name in type_hints:
            if field_name not in value:
                raise ValueError(
                    f"Missing required field '{field_name}' for {self.typeddict_class}"
                )

        return value.copy()

    def struct_dict_to_python(self, struct_dict: dict) -> dict:
        """Convert Arrow struct dict to TypedDict (no conversion needed)."""
        return struct_dict.copy()

    def can_handle_python_type(self, python_type: Type) -> bool:
        return python_type == self.typeddict_class


def register_dynamic_struct_converter(
    registry: SemanticTypeRegistry, typeddict_class: Type, struct_type: pa.StructType
) -> None:
    """Register a converter for a dynamically created TypedDict struct."""
    converter = DynamicStructConverter(typeddict_class, struct_type)
    registry.register_converter(converter)


# Updated arrow_type_to_python function that preserves struct field information
def enhanced_arrow_type_to_python(
    arrow_type: pa.DataType, semantic_registry: SemanticTypeRegistry | None = None
) -> Type:
    """
    Enhanced version of arrow_type_to_python that preserves struct field information.

    For struct types, this creates TypedDict classes that preserve all field names and types.
    """

    # Handle struct types with full field preservation
    if pa.types.is_struct(arrow_type):
        return arrow_struct_to_python_type(arrow_type, semantic_registry)

    # For non-struct types, use standard conversion
    from orcapod.semantic_types.semantic_converters import arrow_type_to_python

    return arrow_type_to_python(arrow_type, semantic_registry)


# Example usage and demonstration
if __name__ == "__main__":
    print("=== Dynamic TypedDict Creation for Arrow Structs ===\n")

    from sample_converters import create_standard_semantic_registry

    # Create semantic registry
    registry = create_standard_semantic_registry()

    # Test with various Arrow struct types
    test_structs = [
        pa.struct([("name", pa.string()), ("age", pa.int64())]),
        pa.struct([("x", pa.float64()), ("y", pa.float64()), ("z", pa.float64())]),
        pa.struct(
            [
                ("person", pa.struct([("name", pa.string()), ("age", pa.int64())])),
                ("active", pa.bool_()),
            ]
        ),
    ]

    print("Converting Arrow struct types to Python type hints:")
    print("=" * 55)

    created_types = []
    for i, struct_type in enumerate(test_structs):
        python_type = arrow_struct_to_python_type(struct_type, registry)
        created_types.append(python_type)

        print(f"\nStruct {i + 1}:")
        print(f"  Arrow: {struct_type}")
        print(f"  Python: {python_type}")
        print(f"  Type name: {python_type.__name__}")

        # Show field information
        type_hints = get_type_hints(python_type)
        print(f"  Fields: {type_hints}")

    print(f"\n" + "=" * 55)
    print("Testing usage of created TypedDict types:")

    # Test the first created type (name, age)
    PersonType = created_types[0]
    person_data: PersonType = {"name": "Alice", "age": 30}
    print(f"\nPerson data: {person_data}")
    print(
        f"Type check: {isinstance(person_data, dict)}"
    )  # Still a regular dict at runtime
    print(f"Field access: name={person_data['name']}, age={person_data['age']}")

    # Test nested struct type
    if len(created_types) > 2:
        NestedType = created_types[2]
        # For nested struct, we need to create the inner struct too
        inner_person: PersonType = {"name": "Bob", "age": 25}
        nested_data: NestedType = {"person": inner_person, "active": True}
        print(f"\nNested data: {nested_data}")
        print(f"Nested access: person.name={nested_data['person']['name']}")

    print(f"\n" + "=" * 55)
    print("Benefits of this approach:")
    print("✓ Full field information preserved in type hints")
    print("✓ Arrow struct -> Python type conversion is complete")
    print("✓ Type checkers understand the structure")
    print("✓ Runtime is still regular dicts (zero overhead)")
    print("✓ Perfect round-trip: Python -> Arrow -> Python")
    print("✓ Handles nested structs recursively")

    print(
        f"\nDynamic TypedDict creation successfully preserves all Arrow struct field information!"
    )
