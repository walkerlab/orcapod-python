"""
Universal Type Conversion Engine for Python ↔ Arrow type bidirectional conversion.

This provides a comprehensive, self-contained system that:
1. Converts Python type hints to Arrow types
2. Converts Arrow types back to Python type hints
3. Creates and caches conversion functions for optimal performance
4. Manages dynamic TypedDict creation for struct preservation
5. Integrates seamlessly with semantic type registries
"""

from typing import TypedDict, Dict, Type, Any, Callable, Tuple, Optional, get_type_hints
import pyarrow as pa
from functools import lru_cache
import hashlib
from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry

# Handle generic types
from typing import get_origin, get_args
import typing


class UniversalTypeConverter:
    """
    Universal engine for Python ↔ Arrow type conversion with cached conversion functions.

    This is a complete, self-contained system that handles:
    - Python type hint → Arrow type conversion
    - Arrow type → Python type hint conversion
    - Dynamic TypedDict creation for struct field preservation
    - Cached conversion function generation
    - Integration with semantic type registries
    """

    def __init__(self, semantic_registry: SemanticTypeRegistry | None = None):
        self.semantic_registry = semantic_registry

        # Cache for created TypedDict classes
        self._struct_signature_to_typeddict: dict[pa.StructType, type] = {}
        self._typeddict_to_struct_signature: dict[type, pa.StructType] = {}
        self._created_type_names: set[str] = set()

        # Cache for conversion functions
        self._python_to_arrow_converters: dict[type, Callable] = {}
        self._arrow_to_python_converters: dict[pa.DataType, Callable] = {}

        # Cache for type mappings
        self._python_to_arrow_types: dict[type, pa.DataType] = {}
        self._arrow_to_python_types: dict[pa.DataType, type] = {}

    def python_type_to_arrow_type(self, python_type: type) -> pa.DataType:
        """
        Convert Python type hint to Arrow type with caching.

        This is the main entry point for Python → Arrow type conversion.
        Results are cached for performance.
        """
        # Check cache first
        if python_type in self._python_to_arrow_types:
            return self._python_to_arrow_types[python_type]

        # Convert and cache result
        arrow_type = self._convert_python_to_arrow(python_type)
        self._python_to_arrow_types[python_type] = arrow_type

        return arrow_type

    def arrow_type_to_python_type(self, arrow_type: pa.DataType) -> Type:
        """
        Convert Arrow type to Python type hint with caching.

        This is the main entry point for Arrow → Python type conversion.
        Results are cached for performance.
        """
        # Check cache first
        if arrow_type in self._arrow_to_python_types:
            return self._arrow_to_python_types[arrow_type]

        # Convert and cache result
        python_type = self._convert_arrow_to_python(arrow_type)
        self._arrow_to_python_types[arrow_type] = python_type

        return python_type

    def get_python_to_arrow_converter(self, python_type: Type) -> Callable[[Any], Any]:
        """
        Get cached conversion function for Python value → Arrow value.

        This creates and caches conversion functions for optimal performance
        during data conversion operations.
        """
        if python_type in self._python_to_arrow_converters:
            return self._python_to_arrow_converters[python_type]

        # Create conversion function
        converter = self._create_python_to_arrow_converter(python_type)
        self._python_to_arrow_converters[python_type] = converter

        return converter

    def get_arrow_to_python_converter(
        self, arrow_type: pa.DataType
    ) -> Callable[[Any], Any]:
        """
        Get cached conversion function for Arrow value → Python value.

        This creates and caches conversion functions for optimal performance
        during data conversion operations.
        """
        if arrow_type in self._arrow_to_python_converters:
            return self._arrow_to_python_converters[arrow_type]

        # Create conversion function
        converter = self._create_arrow_to_python_converter(arrow_type)
        self._arrow_to_python_converters[arrow_type] = converter

        return converter

    def _convert_python_to_arrow(self, python_type: Type) -> pa.DataType:
        """Core Python → Arrow type conversion logic."""

        # Handle basic types
        basic_type_map = {
            int: pa.int64(),
            float: pa.float64(),
            str: pa.large_string(),
            bool: pa.bool_(),
            bytes: pa.large_binary(),
        }

        if python_type in basic_type_map:
            return basic_type_map[python_type]

        # Check semantic registry for registered types
        if self.semantic_registry:
            converter = self.semantic_registry.get_converter_for_python_type(
                python_type
            )
            if converter:
                return converter.arrow_struct_type

        # Handle typeddict look up
        if python_type in self._typeddict_to_struct_signature:
            return self._typeddict_to_struct_signature[python_type]

        # Check generic types
        origin = get_origin(python_type)
        args = get_args(python_type)

        if origin is None:
            # Handle string type names
            if hasattr(python_type, "__name__"):
                type_name = python_type.__name__
                if type_name in basic_type_map:
                    return basic_type_map[type_name]
            raise ValueError(f"Unsupported Python type: {python_type}")

        # Handle list types
        if origin is list:
            if len(args) != 1:
                raise ValueError(
                    f"list type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

        # Handle tuple types
        elif origin is tuple:
            if len(args) == 0:
                raise ValueError("Empty tuple type not supported")

            if len(set(args)) == 1:
                # Homogeneous tuple → fixed-size list
                element_type = self.python_type_to_arrow_type(args[0])
                return pa.list_(element_type, len(args))
            else:
                # Heterogeneous tuple → struct with indexed fields
                fields = []
                for i, arg_type in enumerate(args):
                    field_type = self.python_type_to_arrow_type(arg_type)
                    fields.append((f"f{i}", field_type))
                return pa.struct(fields)

        # Handle dict types
        elif origin is dict:
            if len(args) != 2:
                raise ValueError(
                    f"dict type must have exactly two type arguments, got: {args}"
                )
            key_type = self.python_type_to_arrow_type(args[0])
            value_type = self.python_type_to_arrow_type(args[1])
            key_value_struct = pa.struct([("key", key_type), ("value", value_type)])
            return pa.large_list(key_value_struct)

        # Handle Union/Optional types
        elif origin is typing.Union:
            if len(args) == 2 and type(None) in args:
                # Optional[T] → just T (nullability handled at field level)
                non_none_type = args[0] if args[1] is type(None) else args[1]
                return self.python_type_to_arrow_type(non_none_type)
            else:
                # Complex unions → use first type as fallback
                return self.python_type_to_arrow_type(args[0])

        # Handle set types → lists
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

        else:
            raise ValueError(f"Unsupported generic type: {origin}")

    def _convert_arrow_to_python(self, arrow_type: pa.DataType) -> type | Any:
        """Core Arrow → Python type conversion logic."""

        # Handle basic types
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return str
        elif pa.types.is_boolean(arrow_type):
            return bool
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return bytes

        # Handle struct types
        elif pa.types.is_struct(arrow_type):
            # Check if it's a registered semantic type first
            if self.semantic_registry:
                python_type = (
                    self.semantic_registry.get_python_type_for_struct_signature(
                        arrow_type
                    )
                )
                if python_type:
                    return python_type

            # Create dynamic TypedDict for unregistered struct
            # TODO: add check for heterogeneous tuple checking each field starts with f
            return self._get_or_create_typeddict_for_struct(arrow_type)

        # Handle list types
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            element_type = arrow_type.value_type

            # Check if this is a dict representation: list<struct<key, value>>
            if pa.types.is_struct(element_type):
                field_names = [field.name for field in element_type]
                if set(field_names) == {"key", "value"}:
                    # This is a dict
                    key_field = next(f for f in element_type if f.name == "key")
                    value_field = next(f for f in element_type if f.name == "value")

                    key_python_type = self.arrow_type_to_python_type(key_field.type)
                    value_python_type = self.arrow_type_to_python_type(value_field.type)

                    return dict[key_python_type, value_python_type]

            # Regular list
            element_python_type = self.arrow_type_to_python_type(element_type)

            if pa.types.is_fixed_size_list(arrow_type):
                # Fixed-size list → homogeneous tuple
                size = arrow_type.list_size
                return tuple[tuple(element_python_type for _ in range(size))]
            else:
                # Variable-size list → list
                return list[element_python_type]

        # Handle map types
        elif pa.types.is_map(arrow_type):
            key_python_type = self.arrow_type_to_python_type(arrow_type.key_type)
            value_python_type = self.arrow_type_to_python_type(arrow_type.item_type)
            return dict[key_python_type, value_python_type]

        # Handle union types
        elif pa.types.is_union(arrow_type):
            import typing

            child_types = []
            for i in range(arrow_type.num_fields):
                child_field = arrow_type[i]
                child_types.append(self.arrow_type_to_python_type(child_field.type))

            if len(child_types) == 2 and type(None) in child_types:
                # Optional[T]
                non_none_type = next(t for t in child_types if t is not type(None))
                return typing.Optional[non_none_type]
            else:
                return typing.Union[tuple(child_types)]

        else:
            # Default case for unsupported types
            return Any

    def _get_or_create_typeddict_for_struct(self, struct_type: pa.StructType) -> Type:
        """Get or create a TypedDict class for an Arrow struct type."""

        # Check cache first
        if struct_type in self._struct_signature_to_typeddict:
            return self._struct_signature_to_typeddict[struct_type]

        # Create field specifications for TypedDict
        field_specs: dict[str, type] = {}
        for field in struct_type:
            field_name = field.name
            python_type = self.arrow_type_to_python_type(field.type)
            field_specs[field_name] = python_type

        # Generate unique name
        type_name = self._generate_unique_type_name(field_specs)

        # Create TypedDict dynamically
        typeddict_class = TypedDict(type_name, field_specs)

        # Cache the mapping
        self._struct_signature_to_typeddict[struct_type] = typeddict_class
        self._typeddict_to_struct_signature[typeddict_class] = struct_type

        return typeddict_class

    def _generate_unique_type_name(self, field_specs: Dict[str, Type]) -> str:
        """Generate a unique name for TypedDict based on field specifications."""

        # Create deterministic signature that includes both names and types
        field_items = sorted(field_specs.items())
        signature_parts = []

        for field_name, field_type in field_items:
            type_name = getattr(field_type, "__name__", str(field_type))
            if type_name.startswith("typing."):
                type_name = type_name[7:]
            signature_parts.append(f"{field_name}_{type_name}")

        # Create base name from signature
        if len(signature_parts) <= 2:
            base_name = "Struct_" + "_".join(signature_parts)
        else:
            # Use hash-based approach for larger structs
            signature_str = "_".join(signature_parts)
            signature_hash = hashlib.md5(signature_str.encode()).hexdigest()[:8]
            field_names = [item[0] for item in field_items]

            if len(field_names) <= 3:
                base_name = f"Struct_{'_'.join(field_names)}_{signature_hash}"
            else:
                base_name = f"Struct_{len(field_names)}fields_{signature_hash}"

        # Clean up the name
        base_name = (
            base_name.replace("[", "_")
            .replace("]", "_")
            .replace(",", "_")
            .replace(" ", "")
        )

        self._created_type_names.add(base_name)
        return base_name

    def _create_python_to_arrow_converter(
        self, python_type: type
    ) -> Callable[[Any], Any]:
        """Create a cached conversion function for Python → Arrow values."""

        # Get the Arrow type for this Python type
        arrow_type = self.python_type_to_arrow_type(python_type)

        # Check for semantic type first
        if self.semantic_registry:
            converter = self.semantic_registry.get_converter_for_python_type(
                python_type
            )
            if converter:
                return converter.python_to_struct_dict

        # Create conversion function based on type

        origin = get_origin(python_type)
        args = get_args(python_type)

        if python_type in {int, float, str, bool, bytes} or origin is None:
            # Basic types - no conversion needed
            return lambda value: value

        elif origin is list:
            element_converter = self.get_python_to_arrow_converter(args[0])
            return (
                lambda value: [element_converter(item) for item in value]
                if value
                else []
            )

        elif origin is dict:
            key_converter = self.get_python_to_arrow_converter(args[0])
            value_converter = self.get_python_to_arrow_converter(args[1])
            return (
                lambda value: [
                    {"key": key_converter(k), "value": value_converter(v)}
                    for k, v in value.items()
                ]
                if value
                else []
            )

        elif origin is tuple:
            if len(set(args)) == 1:
                # Homogeneous tuple
                element_converter = self.get_python_to_arrow_converter(args[0])
                return lambda value: [element_converter(item) for item in value]
            else:
                # Heterogeneous tuple
                converters = [self.get_python_to_arrow_converter(arg) for arg in args]
                return lambda value: {
                    f"f{i}": converters[i](item) for i, item in enumerate(value)
                }

        else:
            # Default passthrough
            return lambda value: value

    def _create_arrow_to_python_converter(
        self, arrow_type: pa.DataType
    ) -> Callable[[Any], Any]:
        """Create a cached conversion function for Arrow → Python values."""

        # Get the Python type for this Arrow type
        python_type = self.arrow_type_to_python_type(arrow_type)

        # Check for semantic type first
        if self.semantic_registry and pa.types.is_struct(arrow_type):
            registered_python_type = (
                self.semantic_registry.get_python_type_for_struct_signature(arrow_type)
            )
            if registered_python_type:
                converter = self.semantic_registry.get_converter_for_python_type(
                    registered_python_type
                )
                if converter:
                    return converter.struct_dict_to_python

        # Handle basic types - no conversion needed
        if (
            pa.types.is_integer(arrow_type)
            or pa.types.is_floating(arrow_type)
            or pa.types.is_boolean(arrow_type)
            or pa.types.is_string(arrow_type)
            or pa.types.is_large_string(arrow_type)
            or pa.types.is_binary(arrow_type)
            or pa.types.is_large_binary(arrow_type)
        ):
            return lambda value: value

        # Handle list types
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            element_type = arrow_type.value_type

            # Check if this is a dict representation
            if pa.types.is_struct(element_type):
                field_names = [field.name for field in element_type]
                if set(field_names) == {"key", "value"}:
                    # Dict representation
                    key_field = next(f for f in element_type if f.name == "key")
                    value_field = next(f for f in element_type if f.name == "value")

                    key_converter = self.get_arrow_to_python_converter(key_field.type)
                    value_converter = self.get_arrow_to_python_converter(
                        value_field.type
                    )

                    return (
                        lambda value: {
                            key_converter(item["key"]): value_converter(item["value"])
                            for item in value
                            if item is not None
                        }
                        if value
                        else {}
                    )

            # Regular list
            element_converter = self.get_arrow_to_python_converter(element_type)

            if pa.types.is_fixed_size_list(arrow_type):
                # Fixed-size list → tuple
                return (
                    lambda value: tuple(element_converter(item) for item in value)
                    if value
                    else ()
                )
            else:
                # Variable-size list → list
                return (
                    lambda value: [element_converter(item) for item in value]
                    if value
                    else []
                )

        # Handle struct types (TypedDict)
        elif pa.types.is_struct(arrow_type):
            # Create converters for each field
            field_converters = {}
            for field in arrow_type:
                field_converters[field.name] = self.get_arrow_to_python_converter(
                    field.type
                )

            return (
                lambda value: {
                    field_name: field_converters[field_name](value.get(field_name))
                    for field_name in field_converters
                }
                if value
                else {}
            )

        else:
            # Default passthrough
            return lambda value: value

    def is_dynamic_typeddict(self, python_type: type) -> bool:
        """Check if a type is one of our dynamically created TypedDicts."""
        return python_type in self._typeddict_to_struct_signature

    def get_struct_signature_for_typeddict(
        self, python_type: type
    ) -> pa.StructType | None:
        """Get the Arrow struct signature for a dynamically created TypedDict."""
        return self._typeddict_to_struct_signature.get(python_type)

    def clear_cache(self) -> None:
        """Clear all caches (useful for testing or memory management)."""
        self._struct_signature_to_typeddict.clear()
        self._typeddict_to_struct_signature.clear()
        self._created_type_names.clear()
        self._python_to_arrow_converters.clear()
        self._arrow_to_python_converters.clear()
        self._python_to_arrow_types.clear()
        self._arrow_to_python_types.clear()

    def get_cache_stats(self) -> dict[str, int]:
        """Get statistics about cache usage (useful for debugging/optimization)."""
        return {
            "typeddict_count": len(self._struct_signature_to_typeddict),
            "python_to_arrow_converters": len(self._python_to_arrow_converters),
            "arrow_to_python_converters": len(self._arrow_to_python_converters),
            "type_mappings": len(self._python_to_arrow_types)
            + len(self._arrow_to_python_types),
        }


# Convenience functions that use a global instance
_global_converter: UniversalTypeConverter | None = None


def prepare_arrow_table_to_python_dicts_converter(
    schema: pa.Schema, semantic_registry: SemanticTypeRegistry | None = None
) -> Callable[[pa.Table], list[dict]]:
    """
    Prepare a converter function that converts an Arrow Table to a list of Python dicts.

    This uses the global UniversalTypeConverter instance to handle type conversions.
    """

    # TODO:
    converter = get_global_converter(semantic_registry)

    # construct the converter lookup table to be used as closure
    converter_lut: dict[str, Callable[[Any], Any]] = {}
    for field in schema:
        python_type = converter.arrow_type_to_python_type(field.type)
        python_to_arrow = converter.get_python_to_arrow_converter(python_type)
        converter_lut[field.name] = python_to_arrow

    def schema_specific_converter(table: pa.Table) -> list[dict]:
        result = []
        for row in table.to_pylist():
            converted_row = {k: converter_lut[k](v) for k, v in row.items()}
            result.append(converted_row)
        return result

    return schema_specific_converter


def get_global_converter(
    semantic_registry: SemanticTypeRegistry | None = None,
) -> UniversalTypeConverter:
    """Get or create the global type converter instance."""
    global _global_converter
    if (
        _global_converter is None
        or _global_converter.semantic_registry != semantic_registry
    ):
        _global_converter = UniversalTypeConverter(semantic_registry)
    return _global_converter


# Public API functions
def python_type_to_arrow_type(
    python_type: type, semantic_registry: SemanticTypeRegistry | None = None
) -> pa.DataType:
    """Convert Python type to Arrow type using the global converter."""
    converter = get_global_converter(semantic_registry)
    return converter.python_type_to_arrow_type(python_type)


def arrow_type_to_python_type(
    arrow_type: pa.DataType, semantic_registry: SemanticTypeRegistry | None = None
) -> type:
    """Convert Arrow type to Python type using the global converter."""
    converter = get_global_converter(semantic_registry)
    return converter.arrow_type_to_python_type(arrow_type)


def get_conversion_functions(
    python_type: type, semantic_registry: SemanticTypeRegistry | None = None
) -> tuple[Callable, Callable]:
    """Get both conversion functions for a Python type."""
    converter = get_global_converter(semantic_registry)
    arrow_type = converter.python_type_to_arrow_type(python_type)

    python_to_arrow = converter.get_python_to_arrow_converter(python_type)
    arrow_to_python = converter.get_arrow_to_python_converter(arrow_type)

    return python_to_arrow, arrow_to_python


# Example usage and demonstration
if __name__ == "__main__":
    print("=== Universal Type Conversion Engine ===\n")

    from pathlib import Path
    import uuid
    from sample_converters import create_standard_semantic_registry

    # Create converter with semantic registry
    registry = create_standard_semantic_registry()
    converter = UniversalTypeConverter(registry)

    print("Testing comprehensive type conversion:")
    print("=" * 50)

    # Test various type conversions
    test_types = [
        int,
        str,
        list[int],
        dict[str, float],
        tuple[int, str, bool],
        Path,  # Semantic type
        uuid.UUID,  # Semantic type
    ]

    print("\nType Conversions:")
    for python_type in test_types:
        arrow_type = converter.python_type_to_arrow_type(python_type)
        recovered_type = converter.arrow_type_to_python_type(arrow_type)

        print(f"  {python_type} → {arrow_type} → {recovered_type}")
        print(f"    Round-trip successful: {recovered_type == python_type}")

    print(f"\n" + "=" * 50)
    print("Testing conversion function caching:")

    # Test conversion functions
    test_data = {
        "id": 123,
        "name": "Alice",
        "tags": ["python", "arrow"],
        "metadata": {"active": True, "score": 95.5},
        "file_path": Path("/home/alice/data.csv"),
        "user_id": uuid.uuid4(),
    }

    schema = {
        "id": int,
        "name": str,
        "tags": list[str],
        "metadata": dict[str, Any],
        "file_path": Path,
        "user_id": uuid.UUID,
    }

    # Get conversion functions (these get cached)
    converters = {}
    for field_name, python_type in schema.items():
        python_to_arrow = converter.get_python_to_arrow_converter(python_type)
        arrow_type = converter.python_type_to_arrow_type(python_type)
        arrow_to_python = converter.get_arrow_to_python_converter(arrow_type)
        converters[field_name] = (python_to_arrow, arrow_to_python)

    print("Conversion functions created and cached for all fields")

    # Test round-trip conversion using cached functions
    converted_data = {}
    for field_name, value in test_data.items():
        python_to_arrow, arrow_to_python = converters[field_name]

        # Convert to Arrow format
        arrow_value = python_to_arrow(value)
        # Convert back to Python
        recovered_value = arrow_to_python(arrow_value)

        converted_data[field_name] = recovered_value

        print(
            f"  {field_name}: {type(value).__name__} → Arrow → {type(recovered_value).__name__}"
        )

    print(f"\n" + "=" * 50)
    print("Cache Statistics:")
    stats = converter.get_cache_stats()
    for stat_name, count in stats.items():
        print(f"  {stat_name}: {count}")

    print(f"\n" + "=" * 50)
    print("✅ Universal Type Conversion Engine Benefits:")
    print("✅ Single self-contained system for all conversions")
    print("✅ Holds semantic registry internally")
    print("✅ Caches all conversion functions for performance")
    print("✅ Handles both Python→Arrow and Arrow→Python")
    print("✅ Creates TypedDicts preserving struct field info")
    print("✅ Dramatic reduction in function creation overhead")
    print("✅ Central caching reduces memory usage")
