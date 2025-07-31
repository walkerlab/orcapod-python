import pyarrow as pa
import typing
from typing import get_origin, get_args
import sys

# Basic type mapping for Python -> Arrow conversion
_PYTHON_TO_ARROW_MAP = {
    # Python built-ins
    int: pa.int64(),
    float: pa.float64(),
    str: pa.large_string(),  # Use large_string by default for Polars compatibility
    bool: pa.bool_(),
    bytes: pa.large_binary(),  # Use large_binary by default for Polars compatibility
    # String representations (for when we get type names as strings)
    "int": pa.int64(),
    "float": pa.float64(),
    "str": pa.large_string(),
    "bool": pa.bool_(),
    "bytes": pa.large_binary(),
    # Specific integer types
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    # Specific float types
    "float32": pa.float32(),
    "float64": pa.float64(),
    # Date/time types
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "timestamp": pa.timestamp("us"),
}

# Reverse mapping for Arrow -> Python conversion (handles both regular and large variants)
_ARROW_TO_PYTHON_MAP = {
    # Integer types
    pa.int8(): int,
    pa.int16(): int,
    pa.int32(): int,
    pa.int64(): int,
    pa.uint8(): int,
    pa.uint16(): int,
    pa.uint32(): int,
    pa.uint64(): int,
    # Float types
    pa.float32(): float,
    pa.float64(): float,
    # String types (both regular and large)
    pa.string(): str,
    pa.large_string(): str,
    # Boolean
    pa.bool_(): bool,
    # Binary types (both regular and large)
    pa.binary(): bytes,
    pa.large_binary(): bytes,
}

# Add numpy types if available
try:
    import numpy as np

    _PYTHON_TO_ARROW_MAP.update(
        {
            np.int8: pa.int8(),
            np.int16: pa.int16(),
            np.int32: pa.int32(),
            np.int64: pa.int64(),
            np.uint8: pa.uint8(),
            np.uint16: pa.uint16(),
            np.uint32: pa.uint32(),
            np.uint64: pa.uint64(),
            np.float32: pa.float32(),
            np.float64: pa.float64(),
            np.bool_: pa.bool_(),
        }
    )
except ImportError:
    pass


def python_type_to_arrow(type_hint, semantic_registry=None) -> pa.DataType:
    """
    Convert Python type hints to PyArrow data types.

    Args:
        type_hint: Python type hint to convert
        semantic_registry: Optional semantic type registry to check for semantic types

    Examples:
        list[int] -> pa.large_list(pa.int64())
        tuple[int, int] -> pa.list_(pa.int64(), 2)
        tuple[int, str] -> pa.struct([('f0', pa.int64()), ('f1', pa.large_string())])
        dict[str, int] -> pa.large_list(pa.struct([('key', pa.large_string()), ('value', pa.int64())]))
    """

    # Handle basic types first
    if type_hint in _PYTHON_TO_ARROW_MAP:
        return _PYTHON_TO_ARROW_MAP[type_hint]

    # Check if this is a registered semantic type
    if semantic_registry and hasattr(
        semantic_registry, "get_converter_for_python_type"
    ):
        converter = semantic_registry.get_converter_for_python_type(type_hint)
        if converter:
            return converter.arrow_struct_type

    # Get the origin (e.g., list, tuple, dict) and args (e.g., int, str)
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if origin is None:
        # Handle non-generic types that might not be in basic map
        if hasattr(type_hint, "__name__"):
            type_name = type_hint.__name__
            if type_name in _PYTHON_TO_ARROW_MAP:
                return _PYTHON_TO_ARROW_MAP[type_name]
        raise ValueError(f"Unsupported type: {type_hint}")

    # Handle list types
    if origin is list:
        if len(args) != 1:
            raise ValueError(
                f"list type must have exactly one type argument, got: {args}"
            )
        element_type = python_type_to_arrow(args[0], semantic_registry)
        return pa.large_list(element_type)  # Use large_list for Polars compatibility

    # Handle tuple types
    elif origin is tuple:
        if len(args) == 0:
            raise ValueError("Empty tuple type not supported")

        # Check if all elements are the same type
        if len(set(args)) == 1:
            # Homogeneous tuple: tuple[int, int, int] -> fixed-size list
            element_type = python_type_to_arrow(args[0], semantic_registry)
            return pa.list_(
                element_type, len(args)
            )  # Fixed-size lists are always regular lists
        else:
            # Heterogeneous tuple: tuple[int, str] -> struct with indexed fields
            fields = []
            for i, arg_type in enumerate(args):
                field_type = python_type_to_arrow(arg_type, semantic_registry)
                fields.append((f"f{i}", field_type))
            return pa.struct(fields)

    # Handle dict types
    elif origin is dict:
        if len(args) != 2:
            raise ValueError(
                f"dict type must have exactly two type arguments, got: {args}"
            )
        key_type = python_type_to_arrow(args[0], semantic_registry)
        value_type = python_type_to_arrow(args[1], semantic_registry)

        # Use large_list<struct<key, value>> representation for better compatibility
        # This works reliably across Arrow, Polars, Parquet, etc.
        key_value_struct = pa.struct([("key", key_type), ("value", value_type)])
        return pa.large_list(key_value_struct)

    # Handle Union types (including Optional)
    elif origin is typing.Union:
        # Handle Optional[T] which is Union[T, NoneType]
        if len(args) == 2 and type(None) in args:
            # This is Optional[T]
            non_none_type = args[0] if args[1] is type(None) else args[1]
            base_type = python_type_to_arrow(non_none_type, semantic_registry)
            # PyArrow handles nullability at the field level, so we just return the base type
            return base_type
        else:
            # Complex unions - convert to a union type
            union_types = [python_type_to_arrow(arg, semantic_registry) for arg in args]
            # PyArrow union types are complex - for now, just use the first type as fallback
            # TODO: Implement proper union support when needed
            return union_types[0]  # Simplified - take first type

    else:
        raise ValueError(f"Unsupported generic type: {origin}")


def arrow_type_to_python(arrow_type: pa.DataType) -> type:
    """
    Convert PyArrow data types back to Python type hints.

    Args:
        arrow_type: PyArrow data type to convert

    Returns:
        Python type annotation

    Examples:
        pa.int64() -> int
        pa.large_list(pa.large_string()) -> list[str]
        pa.large_list(pa.struct([('key', pa.large_string()), ('value', pa.int64())])) -> dict[str, int]
        pa.struct([('f0', pa.int64()), ('f1', pa.large_string())]) -> tuple[int, str]

    Raises:
        TypeError: If the Arrow type cannot be converted to a Python type
    """

    # Handle basic types
    if arrow_type in _ARROW_TO_PYTHON_MAP:
        return _ARROW_TO_PYTHON_MAP[arrow_type]

    # Check by Arrow type categories
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

    # Handle complex types
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        element_type = arrow_type.value_type

        # Check if this is a dict representation: list<struct<key: K, value: V>>
        if pa.types.is_struct(element_type):
            field_names = [field.name for field in element_type]

            # Dict pattern: must have exactly 'key' and 'value' fields
            if set(field_names) == {"key", "value"}:
                # Find key and value types
                key_field = next(f for f in element_type if f.name == "key")
                value_field = next(f for f in element_type if f.name == "value")

                key_python_type = arrow_type_to_python(key_field.type)
                value_python_type = arrow_type_to_python(value_field.type)

                return dict[key_python_type, value_python_type]

        # Regular list
        element_python_type = arrow_type_to_python(element_type)

        # Check if this is a fixed-size list (homogeneous tuple representation)
        if (
            hasattr(arrow_type, "list_size") and arrow_type.list_size > 0
        ) or pa.types.is_fixed_size_list(arrow_type):
            # Fixed-size list represents homogeneous tuple
            if pa.types.is_fixed_size_list(arrow_type):
                size = arrow_type.list_size
            else:
                size = arrow_type.list_size
            return tuple[tuple(element_python_type for _ in range(size))]
        else:
            # Variable-size list
            return list[element_python_type]

    elif pa.types.is_struct(arrow_type):
        # Check if this is a heterogeneous tuple representation
        field_names = [field.name for field in arrow_type]

        # Tuple pattern: fields named f0, f1, f2, etc.
        if all(name.startswith("f") and name[1:].isdigit() for name in field_names):
            # Sort by field index to maintain order
            sorted_fields = sorted(arrow_type, key=lambda f: int(f.name[1:]))
            field_types = [arrow_type_to_python(field.type) for field in sorted_fields]
            return tuple[tuple(field_types)]
        else:
            # TODO: Could support NamedTuple or dataclass conversion here
            raise TypeError(
                f"Cannot convert struct type to Python type hint. "
                f"Struct has fields: {field_names}. "
                f"Only tuple-like structs (f0, f1, ...) are supported."
            )

    elif pa.types.is_map(arrow_type):
        # Handle pa.map_ types (though we prefer list<struct> representation)
        key_python_type = arrow_type_to_python(arrow_type.key_type)
        value_python_type = arrow_type_to_python(arrow_type.item_type)
        return dict[key_python_type, value_python_type]

    elif pa.types.is_union(arrow_type):
        # Handle union types -> Union[T1, T2, ...]
        import typing

        # Get the child types from the union
        child_types = []
        for i in range(arrow_type.num_fields):
            child_field = arrow_type[i]
            child_types.append(arrow_type_to_python(child_field.type))

        if len(child_types) == 2 and type(None) in child_types:
            # This is Optional[T]
            non_none_type = next(t for t in child_types if t is not type(None))
            return typing.Optional[non_none_type]
        else:
            return typing.Union[tuple(child_types)]

    else:
        raise TypeError(
            f"Cannot convert Arrow type '{arrow_type}' to Python type hint. "
            f"Supported types: int, float, str, bool, bytes, list, large_list, fixed_size_list, tuple, dict, struct, map, union. "
            f"Arrow type category: {arrow_type}"
        )


def parse_type_string(type_string: str):
    """
    Parse a type hint from a string representation.
    Useful when you have type hints as strings.

    Example:
        parse_type_string("list[int]") -> pa.large_list(pa.int64())
    """
    # This is a simplified version - for production use, consider using ast.literal_eval
    # or a proper type hint parser
    try:
        # Try to evaluate the string as a type hint
        # Note: This uses eval which can be dangerous - use with trusted input only
        import typing

        namespace = {
            "list": list,
            "tuple": tuple,
            "dict": dict,
            "int": int,
            "str": str,
            "float": float,
            "bool": bool,
            "bytes": bytes,
            "Optional": typing.Optional,
            "Union": typing.Union,
        }
        type_hint = eval(type_string, {"__builtins__": {}}, namespace)
        return python_type_to_arrow(type_hint)
    except Exception as e:
        raise ValueError(f"Could not parse type string '{type_string}': {e}")


# Helper functions for dict conversion
def dict_to_arrow_list(d: dict) -> list[dict]:
    """Convert Python dict to Arrow-compatible list of key-value structs."""
    return [{"key": k, "value": v} for k, v in d.items()]


def arrow_list_to_dict(lst: list[dict]) -> dict:
    """Convert Arrow list of key-value structs back to Python dict."""
    return {item["key"]: item["value"] for item in lst if item is not None}


def python_dicts_to_arrow_table(data: list[dict], schema: dict[str, type]) -> pa.Table:
    """
    Convert list of Python dictionaries to PyArrow table with proper type conversion.

    Args:
        data: List of Python dictionaries
        schema: Dictionary mapping field names to Python type hints

    Returns:
        PyArrow table with proper types

    Examples:
        data = [{"x": 5, "y": [1, 2, 3]}, {"x": 9, "y": [2, 4]}]
        schema = {"x": int, "y": list[int]}
        -> PyArrow table with x: int64, y: large_list<int64>

        data = [{"name": "Alice", "scores": {"math": 95, "english": 87}}]
        schema = {"name": str, "scores": dict[str, int]}
        -> PyArrow table with name: large_string, scores: large_list<struct<key, value>>
    """
    if not data:
        raise ValueError("Cannot create table from empty data list")

    if not schema:
        raise ValueError("Schema cannot be empty")

    # Convert schema to Arrow schema
    arrow_fields = []
    for field_name, python_type in schema.items():
        arrow_type = python_type_to_arrow(python_type)
        arrow_fields.append(pa.field(field_name, arrow_type))

    arrow_schema = pa.schema(arrow_fields)

    # Convert data with proper type transformations
    converted_data = []
    for record in data:
        converted_record = {}
        for field_name, python_type in schema.items():
            value = record.get(field_name)
            if value is not None:
                converted_value = _convert_python_value_for_arrow(value, python_type)
                converted_record[field_name] = converted_value
            else:
                converted_record[field_name] = None
        converted_data.append(converted_record)

    # Create table with explicit schema
    try:
        table = pa.table(converted_data, schema=arrow_schema)
        return table
    except Exception as e:
        # Fallback: create each column separately
        arrays = []
        for field in arrow_schema:
            field_name = field.name
            field_type = field.type

            # Extract column data
            column_data = [record.get(field_name) for record in converted_data]

            # Create array with explicit type
            array = pa.array(column_data, type=field_type)
            arrays.append(array)

        return pa.table(arrays, schema=arrow_schema)


def _convert_python_value_for_arrow(value, python_type):
    """
    Convert a Python value to Arrow-compatible format based on expected type.

    Args:
        value: Python value to convert
        python_type: Expected Python type hint

    Returns:
        Value in Arrow-compatible format
    """
    origin = get_origin(python_type)
    args = get_args(python_type)

    # Handle basic types - no conversion needed
    if python_type in {int, float, str, bool, bytes} or origin is None:
        return value

    # Handle Optional types
    if origin is typing.Union and len(args) == 2 and type(None) in args:
        if value is None:
            return None
        non_none_type = args[0] if args[1] is type(None) else args[1]
        return _convert_python_value_for_arrow(value, non_none_type)

    # Handle list types
    elif origin is list:
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"Expected list/tuple for {python_type}, got {type(value)}")
        element_type = args[0]
        return [_convert_python_value_for_arrow(item, element_type) for item in value]

    # Handle tuple types
    elif origin is tuple:
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"Expected list/tuple for {python_type}, got {type(value)}")

        if len(set(args)) == 1:
            # Homogeneous tuple - convert to list
            element_type = args[0]
            return [
                _convert_python_value_for_arrow(item, element_type) for item in value
            ]
        else:
            # Heterogeneous tuple - convert to struct dict
            if len(value) != len(args):
                raise ValueError(
                    f"Tuple length mismatch: expected {len(args)}, got {len(value)}"
                )
            struct_dict = {}
            for i, (item, item_type) in enumerate(zip(value, args)):
                struct_dict[f"f{i}"] = _convert_python_value_for_arrow(item, item_type)
            return struct_dict

    # Handle dict types
    elif origin is dict:
        if not isinstance(value, dict):
            raise TypeError(f"Expected dict for {python_type}, got {type(value)}")

        key_type, value_type = args
        # Convert dict to list of key-value structs
        key_value_list = []
        for k, v in value.items():
            converted_key = _convert_python_value_for_arrow(k, key_type)
            converted_value = _convert_python_value_for_arrow(v, value_type)
            key_value_list.append({"key": converted_key, "value": converted_value})
        return key_value_list

    else:
        # For unsupported types, return as-is and let Arrow handle it
        return value


def arrow_table_to_python_dicts(table: pa.Table) -> list[dict]:
    """
    Convert PyArrow table back to list of Python dictionaries with proper type conversion.

    Args:
        table: PyArrow table to convert

    Returns:
        List of Python dictionaries with proper Python types

    Examples:
        Arrow table with x: int64, y: large_list<int64>
        -> [{"x": 5, "y": [1, 2, 3]}, {"x": 9, "y": [2, 4]}]

        Arrow table with scores: large_list<struct<key: large_string, value: int64>>
        -> [{"name": "Alice", "scores": {"math": 95, "english": 87}}]
    """
    # Convert table to list of raw dictionaries
    raw_dicts = table.to_pylist()

    # Convert each dictionary with proper type transformations
    converted_dicts = []
    for raw_dict in raw_dicts:
        converted_dict = {}
        for field_name, value in raw_dict.items():
            if value is not None:
                # Get the Arrow field type
                field = table.schema.field(field_name)
                arrow_type = field.type

                # Convert based on Arrow type
                converted_value = _convert_arrow_value_to_python(value, arrow_type)
                converted_dict[field_name] = converted_value
            else:
                converted_dict[field_name] = None
        converted_dicts.append(converted_dict)

    return converted_dicts


def _convert_arrow_value_to_python(value, arrow_type):
    """
    Convert Arrow value back to proper Python type.

    Args:
        value: Value from Arrow table (as returned by to_pylist())
        arrow_type: PyArrow type of the field

    Returns:
        Value converted to proper Python type
    """
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
        return value

    # Handle list types (including large_list and fixed_size_list)
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        if value is None:
            return None

        element_type = arrow_type.value_type

        # Check if this is a dict representation: list<struct<key, value>>
        if pa.types.is_struct(element_type):
            field_names = [field.name for field in element_type]
            if set(field_names) == {"key", "value"}:
                # This is a dict - convert list of key-value structs to dict
                result_dict = {}
                for item in value:
                    if item is not None:
                        key_field = element_type.field("key")
                        value_field = element_type.field("value")

                        converted_key = _convert_arrow_value_to_python(
                            item["key"], key_field.type
                        )
                        converted_value = _convert_arrow_value_to_python(
                            item["value"], value_field.type
                        )
                        result_dict[converted_key] = converted_value
                return result_dict

        # Regular list - convert each element
        converted_list = []
        for item in value:
            converted_item = _convert_arrow_value_to_python(item, element_type)
            converted_list.append(converted_item)

        # For fixed-size lists, convert to tuple if all elements are same type
        if pa.types.is_fixed_size_list(arrow_type):
            return tuple(converted_list)
        else:
            return converted_list

    # Handle struct types
    elif pa.types.is_struct(arrow_type):
        if value is None:
            return None

        field_names = [field.name for field in arrow_type]

        # Check if this is a tuple representation (f0, f1, f2, ...)
        if all(name.startswith("f") and name[1:].isdigit() for name in field_names):
            # Convert struct to tuple
            sorted_fields = sorted(arrow_type, key=lambda f: int(f.name[1:]))
            tuple_values = []
            for field in sorted_fields:
                field_value = value.get(field.name)
                converted_value = _convert_arrow_value_to_python(
                    field_value, field.type
                )
                tuple_values.append(converted_value)
            return tuple(tuple_values)
        else:
            # Regular struct - convert each field
            converted_struct = {}
            for field in arrow_type:
                field_name = field.name
                field_value = value.get(field_name)
                converted_value = _convert_arrow_value_to_python(
                    field_value, field.type
                )
                converted_struct[field_name] = converted_value
            return converted_struct

    # Handle map types
    elif pa.types.is_map(arrow_type):
        if value is None:
            return None

        # Maps are returned as list of {'key': k, 'value': v} dicts
        result_dict = {}
        key_type = arrow_type.key_type
        item_type = arrow_type.item_type

        for item in value:
            if item is not None:
                converted_key = _convert_arrow_value_to_python(item["key"], key_type)
                converted_value = _convert_arrow_value_to_python(
                    item["value"], item_type
                )
                result_dict[converted_key] = converted_value
        return result_dict

    else:
        # For unsupported types, return as-is
        return value


if __name__ == "__main__":
    print("=== Complete Python Type Hint ↔ PyArrow Type Converter ===\n")

    # Test basic functionality first
    print("Testing basic round-trip:")
    try:
        # Simple test
        python_type = dict[str, int]
        arrow_type = python_type_to_arrow(python_type)
        recovered_type = arrow_type_to_python(arrow_type)
        print(f"✓ {python_type} -> {arrow_type} -> {recovered_type}")
        print(f"  Match: {recovered_type == python_type}")
    except Exception as e:
        print(f"✗ Basic test failed: {e}")

    print("\n" + "=" * 60)
    print("Testing complex nested structures:")

    complex_nested_tests = [
        # Nested dictionaries
        (
            dict[str, dict[str, int]],
            pa.large_list(
                pa.struct(
                    [
                        ("key", pa.large_string()),
                        (
                            "value",
                            pa.large_list(
                                pa.struct(
                                    [("key", pa.large_string()), ("value", pa.int64())]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        # Mixed complex types in tuples
        (
            tuple[dict[str, int], list[str]],
            pa.struct(
                [
                    (
                        "f0",
                        pa.large_list(
                            pa.struct(
                                [("key", pa.large_string()), ("value", pa.int64())]
                            )
                        ),
                    ),
                    ("f1", pa.large_list(pa.large_string())),
                ]
            ),
        ),
        # Complex value types in dicts
        (
            dict[str, list[int]],
            pa.large_list(
                pa.struct(
                    [("key", pa.large_string()), ("value", pa.large_list(pa.int64()))]
                )
            ),
        ),
        # Triple nesting
        (
            list[dict[str, list[int]]],
            pa.large_list(
                pa.large_list(
                    pa.struct(
                        [
                            ("key", pa.large_string()),
                            ("value", pa.large_list(pa.int64())),
                        ]
                    )
                )
            ),
        ),
        # Complex tuple with nested structures
        (
            tuple[list[int], dict[str, float], str],
            pa.struct(
                [
                    ("f0", pa.large_list(pa.int64())),
                    (
                        "f1",
                        pa.large_list(
                            pa.struct(
                                [("key", pa.large_string()), ("value", pa.float64())]
                            )
                        ),
                    ),
                    ("f2", pa.large_string()),
                ]
            ),
        ),
    ]

    for python_type, expected_arrow_type in complex_nested_tests:
        try:
            result = python_type_to_arrow(python_type)
            success = result == expected_arrow_type
            status = "✓" if success else "✗"
            print(f"{status} {python_type}")
            print(f"    -> {result}")
            if not success:
                print(f"    Expected: {expected_arrow_type}")
        except Exception as e:
            print(f"✗ {python_type} -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing complex nested round-trips:")

    complex_round_trip_tests = [
        dict[str, dict[str, int]],
        tuple[dict[str, int], list[str]],
        dict[str, list[int]],
        list[dict[str, list[int]]],
        tuple[list[int], dict[str, float], str],
        dict[str, tuple[int, str]],
        list[tuple[dict[str, int], list[str]]],
    ]

    for python_type in complex_round_trip_tests:
        try:
            # Python -> Arrow -> Python
            arrow_type = python_type_to_arrow(python_type)
            recovered_python_type = arrow_type_to_python(arrow_type)
            success = recovered_python_type == python_type
            status = "✓" if success else "✗"
            print(f"{status} {python_type}")
            print(f"    -> {arrow_type}")
            print(f"    -> {recovered_python_type}")
            if not success:
                print(f"    Round-trip failed!")
        except Exception as e:
            print(f"✗ {python_type} -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing Python -> Arrow conversion:")

    # Test cases for Python -> Arrow
    python_to_arrow_tests = [
        # Basic types
        (int, pa.int64()),
        (str, pa.large_string()),
        (float, pa.float64()),
        (bool, pa.bool_()),
        # Lists (both regular and large)
        (list[int], pa.large_list(pa.int64())),
        (list[str], pa.large_list(pa.large_string())),
        (list[float], pa.large_list(pa.float64())),
        # Homogeneous tuples (always use regular fixed-size lists)
        (tuple[int, int], pa.list_(pa.int64(), 2)),
        (tuple[str, str, str], pa.list_(pa.large_string(), 3)),
        # Heterogeneous tuples
        (tuple[int, str], pa.struct([("f0", pa.int64()), ("f1", pa.large_string())])),
        (
            tuple[int, str, float],
            pa.struct(
                [("f0", pa.int64()), ("f1", pa.large_string()), ("f2", pa.float64())]
            ),
        ),
        # Dict types - using large_list<struct<key, value>> for Polars compatibility
        (
            dict[str, int],
            pa.large_list(
                pa.struct([("key", pa.large_string()), ("value", pa.int64())])
            ),
        ),
        (
            dict[int, str],
            pa.large_list(
                pa.struct([("key", pa.int64()), ("value", pa.large_string())])
            ),
        ),
        # Nested types
        (list[list[int]], pa.large_list(pa.large_list(pa.int64()))),
        (
            list[tuple[int, str]],
            pa.large_list(pa.struct([("f0", pa.int64()), ("f1", pa.large_string())])),
        ),
    ]

    for python_type, expected_arrow_type in python_to_arrow_tests:
        try:
            result = python_type_to_arrow(python_type)
            success = result == expected_arrow_type
            status = "✓" if success else "✗"
            print(f"{status} {python_type} -> {result}")
            if not success:
                print(f"    Expected: {expected_arrow_type}")
        except Exception as e:
            print(f"✗ {python_type} -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing Arrow -> Python type conversion:")

    arrow_to_python_tests = [
        # Basic types (both regular and large variants)
        (pa.int64(), int),
        (pa.string(), str),
        (pa.large_string(), str),
        (pa.float64(), float),
        (pa.bool_(), bool),
        (pa.binary(), bytes),
        (pa.large_binary(), bytes),
        # Lists (both regular and large)
        (pa.list_(pa.int64(), -1), list[int]),
        (pa.large_list(pa.int64()), list[int]),
        (pa.list_(pa.string(), -1), list[str]),
        (pa.large_list(pa.large_string()), list[str]),
        # Fixed-size lists (homogeneous tuples)
        (pa.list_(pa.int64(), 3), tuple[int, int, int]),
        (pa.list_(pa.large_string(), 2), tuple[str, str]),
        # Dict representation: both regular and large list variants
        (
            pa.list_(pa.struct([("key", pa.string()), ("value", pa.int64())]), -1),
            dict[str, int],
        ),
        (
            pa.large_list(
                pa.struct([("key", pa.large_string()), ("value", pa.int64())])
            ),
            dict[str, int],
        ),
        (
            pa.list_(pa.struct([("key", pa.int64()), ("value", pa.string())]), -1),
            dict[int, str],
        ),
        (
            pa.large_list(
                pa.struct([("key", pa.int64()), ("value", pa.large_string())])
            ),
            dict[int, str],
        ),
        # Heterogeneous tuples: struct<f0, f1, ...>
        (pa.struct([("f0", pa.int64()), ("f1", pa.string())]), tuple[int, str]),
        (pa.struct([("f0", pa.int64()), ("f1", pa.large_string())]), tuple[int, str]),
        (
            pa.struct([("f0", pa.int64()), ("f1", pa.string()), ("f2", pa.float64())]),
            tuple[int, str, float],
        ),
        # Maps (if encountered)
        (pa.map_(pa.string(), pa.int64()), dict[str, int]),
        (pa.map_(pa.large_string(), pa.int64()), dict[str, int]),
        # Nested structures
        (pa.list_(pa.list_(pa.int64(), -1), -1), list[list[int]]),
        (pa.large_list(pa.large_list(pa.int64())), list[list[int]]),
    ]

    for arrow_type, expected_python_type in arrow_to_python_tests:
        try:
            result = arrow_type_to_python(arrow_type)
            success = result == expected_python_type
            status = "✓" if success else "✗"
            print(f"{status} {arrow_type} -> {result}")
            if not success:
                print(f"    Expected: {expected_python_type}")
        except Exception as e:
            print(f"✗ {arrow_type} -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing round-trip conversion:")

    round_trip_tests = [
        dict[str, int],
        list[int],
        tuple[int, str],
        tuple[str, str, str],
        list[dict[str, int]],
        list[list[str]],
        tuple[int, float, bool],
    ]

    for python_type in round_trip_tests:
        try:
            # Python -> Arrow -> Python
            arrow_type = python_type_to_arrow(python_type)
            recovered_python_type = arrow_type_to_python(arrow_type)
            success = recovered_python_type == python_type
            status = "✓" if success else "✗"
            print(f"{status} {python_type} -> {arrow_type} -> {recovered_python_type}")
            if not success:
                print(f"    Round-trip failed!")
        except Exception as e:
            print(f"✗ {python_type} -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing string parsing:")

    string_tests = [
        "list[int]",
        "tuple[int, str]",
        "dict[str, int]",
        "list[dict[str, float]]",
    ]

    for type_str in string_tests:
        try:
            result = parse_type_string(type_str)
            print(f"✓ '{type_str}' -> {result}")
        except Exception as e:
            print(f"✗ '{type_str}' -> ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing practical data conversion:")

    # Test actual data conversion
    try:
        # Create some test data
        test_data = [
            {"name": "Alice", "scores": {"math": 95, "english": 87}},
            {"name": "Bob", "scores": {"math": 78, "english": 92}},
        ]

        # Create schema with nested dict using large_list<struct> representation
        dict_type = python_type_to_arrow(dict[str, int])
        schema = pa.schema([("name", pa.large_string()), ("scores", dict_type)])

        print(f"Dict type representation: {dict_type}")

        # Convert Python dicts to the expected list<struct> format
        converted_data = []
        for record in test_data:
            converted_record = record.copy()
            if "scores" in converted_record:
                # Convert dict to list of key-value structs
                scores_dict = converted_record["scores"]
                converted_record["scores"] = dict_to_arrow_list(scores_dict)
            converted_data.append(converted_record)

        # Create Arrow table - need to handle the conversion properly
        try:
            table = pa.table(converted_data, schema=schema)
        except Exception as table_error:
            # If direct conversion fails, convert each column separately
            print(f"  Direct table creation failed: {table_error}")
            print("  Trying column-by-column conversion...")

            # Convert each field separately
            arrays = []
            for field in schema:
                field_name = field.name
                field_type = field.type

                # Extract column data
                column_data = [record.get(field_name) for record in converted_data]

                # Create array with explicit type
                array = pa.array(column_data, type=field_type)
                arrays.append(array)

            # Create table from arrays
            table = pa.table(arrays, schema=schema)
        print(f"✓ Created PyArrow table with large_list<struct> representation")

        # Convert back to Python and reconstruct dicts
        result_data = table.to_pylist()
        for record in result_data:
            if "scores" in record and record["scores"]:
                # Convert list of key-value structs back to dict
                record["scores"] = arrow_list_to_dict(record["scores"])

        print(f"✓ Round-trip successful: {result_data[0]['scores']}")

    except Exception as e:
        print(f"✗ Practical conversion test failed: {e}")

    print("Testing edge cases and limitations:")

    edge_case_tests = [
        # Complex key types - these are challenging but let's see what happens
        "dict[tuple[str, int], str]",  # tuple keys
        "dict[str, dict[int, list[str]]]",  # deeply nested
        "Optional[dict[str, int]]",  # optional complex types
    ]

    for type_str in edge_case_tests:
        try:
            # Parse and convert
            namespace = {
                "list": list,
                "tuple": tuple,
                "dict": dict,
                "int": int,
                "str": str,
                "float": float,
                "bool": bool,
                "bytes": bytes,
                "Optional": typing.Optional,
                "Union": typing.Union,
            }
            python_type = eval(type_str, {"__builtins__": {}}, namespace)
            arrow_type = python_type_to_arrow(python_type)
            recovered_type = arrow_type_to_python(arrow_type)

            success = recovered_type == python_type
            status = "✓" if success else "⚠"
            print(f"{status} {type_str}")
            print(f"    -> {arrow_type}")
            print(f"    -> {recovered_type}")
            if not success:
                print(f"    Note: Complex key types may have limitations")

        except Exception as e:
            print(f"✗ {type_str} -> ERROR: {e}")

    print(f"\n{'=' * 60}")
    print("All tests completed!")
