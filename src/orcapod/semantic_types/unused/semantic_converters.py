import pyarrow as pa
from typing import get_origin, get_args, Any
import typing
from collections.abc import Collection, Sequence, Mapping, Iterable, Set
from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry
from orcapod.types import PythonSchema


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


def python_type_to_arrow(
    type_hint, semantic_registry: SemanticTypeRegistry | None = None
) -> pa.DataType:
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
        Path -> pa.struct([('path', pa.large_string())]) # if registered in semantic registry
    """

    # Handle basic types first
    if type_hint in _PYTHON_TO_ARROW_MAP:
        return _PYTHON_TO_ARROW_MAP[type_hint]

    # Check if this is a registered semantic type
    if semantic_registry is not None:
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

    # Handle abstract base classes and collections
    elif origin in {Collection, Sequence, Iterable}:
        # Treat as list - most common concrete implementation
        if len(args) != 1:
            raise ValueError(
                f"{origin.__name__} type must have exactly one type argument, got: {args}"
            )
        element_type = python_type_to_arrow(args[0], semantic_registry)
        return pa.large_list(element_type)

    elif origin is Set or origin is set:
        # Sets -> lists (Arrow doesn't have native set type)
        if len(args) != 1:
            raise ValueError(
                f"set type must have exactly one type argument, got: {args}"
            )
        element_type = python_type_to_arrow(args[0], semantic_registry)
        return pa.large_list(element_type)

    elif origin is Mapping:
        # Mapping -> dict representation
        if len(args) != 2:
            raise ValueError(
                f"Mapping type must have exactly two type arguments, got: {args}"
            )
        key_type = python_type_to_arrow(args[0], semantic_registry)
        value_type = python_type_to_arrow(args[1], semantic_registry)
        key_value_struct = pa.struct([("key", key_type), ("value", value_type)])
        return pa.large_list(key_value_struct)
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


def python_schema_to_arrow(
    python_schema: PythonSchema, semantic_registry: SemanticTypeRegistry | None = None
) -> pa.Schema:
    """
    Convert a Python schema (TypeSpec) to a PyArrow schema.

    Args:
        python_schema: TypeSpec representing the Python schema
        semantic_registry: Optional semantic type registry to check for semantic types

    Returns:
        PyArrow Schema object

    Raises:
        ValueError: If the Python schema cannot be converted to Arrow schema
    """

    arrow_fields = []
    for field_name, field_type in python_schema.items():
        arrow_type = python_type_to_arrow(
            field_type, semantic_registry=semantic_registry
        )
        arrow_fields.append(pa.field(field_name, arrow_type))
    return pa.schema(arrow_fields)


def arrow_type_to_python(
    arrow_type: pa.DataType, semantic_registry: SemanticTypeRegistry | None = None
) -> type:
    """
    Convert PyArrow data types back to Python type hints.

    Args:
        arrow_type: PyArrow data type to convert
        semantic_registry: Optional semantic type registry for semantic types

    Returns:
        Python type annotation

    Examples:
        pa.int64() -> int
        pa.large_list(pa.large_string()) -> list[str]
        pa.large_list(pa.struct([('key', pa.large_string()), ('value', pa.int64())])) -> dict[str, int]
        pa.struct([('f0', pa.int64()), ('f1', pa.large_string())]) -> tuple[int, str]
        pa.struct([('path', pa.large_string())]) -> Path # if registered in semantic registry

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

                key_python_type = arrow_type_to_python(
                    key_field.type, semantic_registry
                )
                value_python_type = arrow_type_to_python(
                    value_field.type, semantic_registry
                )

                return dict[key_python_type, value_python_type]

        # Regular list
        element_python_type = arrow_type_to_python(element_type, semantic_registry)

        # Check if this is a fixed-size list (homogeneous tuple representation)
        if pa.types.is_fixed_size_list(arrow_type):
            # Fixed-size list -> homogeneous tuple
            size = arrow_type.list_size
            return tuple[tuple(element_python_type for _ in range(size))]
        else:
            # Variable-size list -> list
            return list[element_python_type]

    elif pa.types.is_struct(arrow_type):
        # First check if this is a semantic type using struct signature recognition
        if semantic_registry:
            python_type = semantic_registry.get_python_type_for_struct_signature(
                arrow_type
            )
            if python_type:
                return python_type

        # Check if this is a heterogeneous tuple representation
        field_names = [field.name for field in arrow_type]

        # Tuple pattern: fields named f0, f1, f2, etc.
        if all(name.startswith("f") and name[1:].isdigit() for name in field_names):
            # Sort by field index to maintain order
            sorted_fields = sorted(arrow_type, key=lambda f: int(f.name[1:]))
            field_types = [
                arrow_type_to_python(field.type, semantic_registry)
                for field in sorted_fields
            ]
            return tuple[tuple(field_types)]
        else:
            # Unknown struct type - cannot convert
            raise TypeError(
                f"Cannot convert struct type to Python type hint. "
                f"Struct has fields: {field_names}. "
                f"Only tuple-like structs (f0, f1, ...) or registered semantic structs are supported."
            )

    elif pa.types.is_map(arrow_type):
        # Handle pa.map_ types (though we prefer list<struct> representation)
        key_python_type = arrow_type_to_python(arrow_type.key_type, semantic_registry)
        value_python_type = arrow_type_to_python(
            arrow_type.item_type, semantic_registry
        )
        return dict[key_python_type, value_python_type]

    elif pa.types.is_union(arrow_type):
        # Handle union types -> Union[T1, T2, ...]
        import typing

        # Get the child types from the union
        child_types = []
        for i in range(arrow_type.num_fields):
            child_field = arrow_type[i]
            child_types.append(
                arrow_type_to_python(child_field.type, semantic_registry)
            )

        if len(child_types) == 2 and type(None) in child_types:
            # This is Optional[T]
            non_none_type = next(t for t in child_types if t is not type(None))
            return typing.Optional[non_none_type]  # type: ignore
        else:
            return typing.Union[tuple(child_types)]  # type: ignore

    else:
        raise TypeError(
            f"Cannot convert Arrow type '{arrow_type}' to Python type hint. "
            f"Supported types: int, float, str, bool, bytes, list, large_list, fixed_size_list, tuple, dict, struct, map, union. "
            f"Arrow type category: {arrow_type}"
        )


def arrow_schema_to_python(
    arrow_schema: pa.Schema, semantic_registry: SemanticTypeRegistry | None = None
) -> PythonSchema:
    """
    Convert a PyArrow schema to a Python schema (TypeSpec).

    Args:
        arrow_schema: PyArrow Schema object
        semantic_registry: Optional semantic type registry for semantic types

    Returns:
        TypeSpec representing the Python schema

    Raises:
        TypeError: If the Arrow schema cannot be converted to Python schema
    """
    return {
        field.name: arrow_type_to_python(field.type, semantic_registry)
        for field in arrow_schema
    }


def parse_type_string(
    type_string: str, semantic_registry: SemanticTypeRegistry | None = None
):
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
        return python_type_to_arrow(type_hint, semantic_registry)
    except Exception as e:
        raise ValueError(f"Could not parse type string '{type_string}': {e}")


def infer_schema_from_data(
    data: list[dict], semantic_registry: SemanticTypeRegistry | None = None
) -> dict[str, type]:
    """
    Infer schema from sample data (best effort).

    Args:
        data: List of sample dictionaries
        semantic_registry: Optional semantic type registry for detecting semantic types

    Returns:
        Dictionary mapping field names to inferred Python types

    Note: This is best-effort inference and may not handle all edge cases.
    For production use, explicit schemas are recommended.
    """
    if not data:
        return {}

    schema = {}

    # Get all possible field names
    all_fields = set()
    for record in data:
        all_fields.update(record.keys())

    # Infer type for each field
    for field_name in all_fields:
        field_values = [
            record.get(field_name)
            for record in data
            if field_name in record and record[field_name] is not None
        ]

        if not field_values:
            schema[field_name] = str  # Default fallback instead of Any
            continue

        # Get types of all values
        value_types = {type(v) for v in field_values}

        if len(value_types) == 1:
            # All values have same type
            value_type = next(iter(value_types))

            # Check if this is a semantic type first
            if semantic_registry:
                converter = semantic_registry.get_converter_for_python_type(value_type)
                if converter:
                    schema[field_name] = value_type
                    continue

            # For containers, try to infer element types
            if value_type is list and field_values:
                # Infer list element type from first non-empty list
                for lst in field_values:
                    if lst:  # non-empty list
                        element_types = {type(elem) for elem in lst}
                        if len(element_types) == 1:
                            element_type = next(iter(element_types))
                            schema[field_name] = list[element_type]
                        else:
                            # Mixed types - use str as fallback instead of Any
                            schema[field_name] = list[str]
                        break
                else:
                    schema[field_name] = list[str]  # Default fallback instead of Any

            elif value_type in {set, frozenset} and field_values:
                # Infer set element type from first non-empty set
                for s in field_values:
                    if s:  # non-empty set
                        element_types = {type(elem) for elem in s}
                        if len(element_types) == 1:
                            element_type = next(iter(element_types))
                            schema[field_name] = set[element_type]
                        else:
                            schema[field_name] = set[
                                str
                            ]  # Mixed types - fallback to str
                        break
                else:
                    schema[field_name] = set[str]  # All sets empty - fallback to str

            elif value_type is dict and field_values:
                # Infer dict types from first non-empty dict
                for d in field_values:
                    if d:  # non-empty dict
                        key_types = {type(k) for k in d.keys()}
                        value_types = {type(v) for v in d.values()}

                        if len(key_types) == 1 and len(value_types) == 1:
                            key_type = next(iter(key_types))
                            val_type = next(iter(value_types))
                            schema[field_name] = dict[key_type, val_type]
                        else:
                            # Mixed types - use most common types or fallback to str
                            key_type = (
                                str if str in key_types else next(iter(key_types))
                            )
                            val_type = (
                                str if str in value_types else next(iter(value_types))
                            )
                            schema[field_name] = dict[key_type, val_type]
                        break
                else:
                    schema[field_name] = dict[
                        str, str
                    ]  # Default fallback instead of Any

            else:
                schema[field_name] = value_type

        else:
            # Mixed types - use str as fallback instead of Any
            schema[field_name] = str

    return schema


def arrow_list_to_set(lst: list) -> set:
    """Convert Arrow list back to Python set (removes duplicates)."""
    return set(lst) if lst is not None else set()


def dict_to_arrow_list(d: dict) -> list[dict]:
    """Convert Python dict to Arrow-compatible list of key-value structs."""
    return [{"key": k, "value": v} for k, v in d.items()]


def arrow_list_to_dict(lst: list[dict]) -> dict:
    """Convert Arrow list of key-value structs back to Python dict."""
    return {item["key"]: item["value"] for item in lst if item is not None}


def python_dicts_to_arrow_table(
    data: list[dict],
    schema: dict[str, type] | None = None,
    semantic_registry: SemanticTypeRegistry | None = None,
) -> pa.Table:
    """
    Convert list of Python dictionaries to PyArrow table with proper type conversion.

    Args:
        data: List of Python dictionaries
        schema: Dictionary mapping field names to Python type hints (optional)
        semantic_registry: Optional semantic type registry for complex Python objects

    Returns:
        PyArrow table with proper types

    Examples:
        # Basic usage
        data = [{"x": 5, "y": [1, 2, 3]}, {"x": 9, "y": [2, 4]}]
        schema = {"x": int, "y": list[int]}

        # With semantic types
        from pathlib import Path
        data = [{"name": "Alice", "file": Path("/home/alice/data.csv")}]
        schema = {"name": str, "file": Path}
        table = python_dicts_to_arrow_table(data, schema, semantic_registry)
    """
    if not data:
        raise ValueError("Cannot create table from empty data list")

    # Auto-infer schema if not provided
    if schema is None:
        schema = infer_schema_from_data(data, semantic_registry)
        print(f"Auto-inferred schema: {schema}")

    if not schema:
        raise ValueError("Schema cannot be empty (and could not be inferred)")

    # Convert schema to Arrow schema (with semantic type support)
    arrow_fields = []
    for field_name, python_type in schema.items():
        arrow_type = python_type_to_arrow(python_type, semantic_registry)
        arrow_fields.append(pa.field(field_name, arrow_type))

    arrow_schema = pa.schema(arrow_fields)

    # Convert data with proper type transformations (with semantic type support)
    converted_data = []
    for record in data:
        converted_record = {}
        for field_name, python_type in schema.items():
            value = record.get(field_name)
            if value is not None:
                converted_value = _convert_python_value_for_arrow(
                    value, python_type, semantic_registry
                )
                converted_record[field_name] = converted_value
            else:
                converted_record[field_name] = None
        converted_data.append(converted_record)

    # Create table with explicit schema
    try:
        table = pa.table(converted_data, schema=arrow_schema)
        return table
    except Exception:
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


def _convert_python_value_for_arrow(
    value, python_type, semantic_registry: SemanticTypeRegistry | None = None
):
    """
    Convert a Python value to Arrow-compatible format based on expected type.

    Args:
        value: Python value to convert
        python_type: Expected Python type hint
        semantic_registry: Optional semantic type registry

    Returns:
        Value in Arrow-compatible format
    """
    # First, check if this is a semantic type
    if semantic_registry:
        converter = semantic_registry.get_converter_for_python_type(python_type)
        if converter:
            # Convert using semantic type converter
            return converter.python_to_struct_dict(value)

    # Fall back to standard type conversion
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
        return _convert_python_value_for_arrow(value, non_none_type, semantic_registry)

    # Handle abstract collections
    elif origin is list or origin in {Collection, Sequence, Iterable}:
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"Expected list/tuple for {python_type}, got {type(value)}")
        element_type = args[0] if args else Any
        return [
            _convert_python_value_for_arrow(item, element_type, semantic_registry)
            for item in value
        ]

    # Handle set types
    elif origin is set or origin is Set:
        if not isinstance(value, (set, frozenset, list, tuple)):
            raise TypeError(
                f"Expected set/list/tuple for {python_type}, got {type(value)}"
            )
        element_type = args[0] if args else Any

        # Convert set to sorted list for deterministic ordering
        if isinstance(value, (set, frozenset)):
            try:
                # Sort if elements are comparable
                value_list = sorted(value)
            except TypeError:
                # If elements aren't comparable (e.g., mixed types), convert to list as-is
                # This maintains some order but isn't guaranteed to be deterministic
                value_list = list(value)
        else:
            # Already a list/tuple, keep as-is
            value_list = list(value)

        return [
            _convert_python_value_for_arrow(item, element_type, semantic_registry)
            for item in value_list
        ]

    # Handle mapping types
    elif origin is dict or origin is Mapping:
        if not isinstance(value, dict):
            raise TypeError(f"Expected dict for {python_type}, got {type(value)}")

        key_type, value_type = (args[0], args[1]) if len(args) >= 2 else (Any, Any)
        # Convert dict to list of key-value structs
        key_value_list = []
        for k, v in value.items():
            converted_key = _convert_python_value_for_arrow(
                k, key_type, semantic_registry
            )
            converted_value = _convert_python_value_for_arrow(
                v, value_type, semantic_registry
            )
            key_value_list.append({"key": converted_key, "value": converted_value})
        return key_value_list

    # Handle tuple types
    elif origin is tuple:
        if not isinstance(value, (list, tuple)):
            raise TypeError(f"Expected list/tuple for {python_type}, got {type(value)}")

        if len(set(args)) == 1:
            # Homogeneous tuple - convert to list
            element_type = args[0]
            return [
                _convert_python_value_for_arrow(item, element_type, semantic_registry)
                for item in value
            ]
        else:
            # Heterogeneous tuple - convert to struct dict
            if len(value) != len(args):
                raise ValueError(
                    f"Tuple length mismatch: expected {len(args)}, got {len(value)}"
                )
            struct_dict = {}
            for i, (item, item_type) in enumerate(zip(value, args)):
                struct_dict[f"f{i}"] = _convert_python_value_for_arrow(
                    item, item_type, semantic_registry
                )
            return struct_dict

    # Handle dict types
    elif origin is dict:
        if not isinstance(value, dict):
            raise TypeError(f"Expected dict for {python_type}, got {type(value)}")

        key_type, value_type = args
        # Convert dict to list of key-value structs
        key_value_list = []
        for k, v in value.items():
            converted_key = _convert_python_value_for_arrow(
                k, key_type, semantic_registry
            )
            converted_value = _convert_python_value_for_arrow(
                v, value_type, semantic_registry
            )
            key_value_list.append({"key": converted_key, "value": converted_value})
        return key_value_list

    else:
        # For unsupported types, return as-is and let Arrow handle it
        return value


def arrow_table_to_python_dicts(
    table: pa.Table, semantic_registry: SemanticTypeRegistry | None = None
) -> list[dict]:
    """
    Convert PyArrow table back to list of Python dictionaries with proper type conversion.

    Args:
        table: PyArrow table to convert
        semantic_registry: Optional semantic type registry for complex Python objects

    Returns:
        List of Python dictionaries with proper Python types

    Examples:
        Arrow table with x: int64, y: large_list<int64>
        -> [{"x": 5, "y": [1, 2, 3]}, {"x": 9, "y": [2, 4]}]

        Arrow table with semantic types (Path stored as struct)
        -> [{"name": "Alice", "file": Path("/home/alice/data.csv")}]
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

                # Convert based on Arrow type (with semantic type support)
                converted_value = _convert_arrow_value_to_python(
                    value, arrow_type, semantic_registry
                )
                converted_dict[field_name] = converted_value
            else:
                converted_dict[field_name] = None
        converted_dicts.append(converted_dict)

    return converted_dicts


def _convert_arrow_value_to_python(
    value, arrow_type, semantic_registry: SemanticTypeRegistry | None = None
):
    """
    Convert Arrow value back to proper Python type.

    Args:
        value: Value from Arrow table (as returned by to_pylist())
        arrow_type: PyArrow type of the field
        semantic_registry: Optional semantic type registry

    Returns:
        Value converted to proper Python type
    """
    # First, check if this is a semantic struct type using signature recognition
    if semantic_registry and pa.types.is_struct(arrow_type):
        python_type = semantic_registry.get_python_type_for_struct_signature(arrow_type)
        if python_type:
            converter = semantic_registry.get_converter_for_python_type(python_type)
            if converter and isinstance(value, dict):
                # Convert using semantic type converter
                return converter.struct_dict_to_python(value)

    # Fall back to standard type conversion
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
                            item["key"], key_field.type, semantic_registry
                        )
                        converted_value = _convert_arrow_value_to_python(
                            item["value"], value_field.type, semantic_registry
                        )
                        result_dict[converted_key] = converted_value
                return result_dict

        # Regular list - convert each element
        converted_list = []
        for item in value:
            converted_item = _convert_arrow_value_to_python(
                item, element_type, semantic_registry
            )
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
                    field_value, field.type, semantic_registry
                )
                tuple_values.append(converted_value)
            return tuple(tuple_values)
        else:
            # Regular struct - convert each field (could be semantic type handled above)
            converted_struct = {}
            for field in arrow_type:
                field_name = field.name
                field_value = value.get(field_name)
                converted_value = _convert_arrow_value_to_python(
                    field_value, field.type, semantic_registry
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
                converted_key = _convert_arrow_value_to_python(
                    item["key"], key_type, semantic_registry
                )
                converted_value = _convert_arrow_value_to_python(
                    item["value"], item_type, semantic_registry
                )
                result_dict[converted_key] = converted_value
        return result_dict

    else:
        # For unsupported types, return as-is
        return value


if __name__ == "__main__":
    print("=== Semantic Type System with Struct Signature Recognition ===\n")

    # This system now uses struct signature recognition instead of special marker fields
    print("Key improvements:")
    print("- Clean, self-documenting struct schemas")
    print("- Zero storage overhead (no marker fields)")
    print("- Natural field names for user queries")
    print("- Struct signature uniquely identifies semantic types")
    print("- Registry maps Python types ↔ struct signatures")

    print("\n" + "=" * 60)
    print("Example struct signatures:")
    print("Path: struct<path: large_string>")
    print("UUID: struct<uuid: string>")
    print("Email: struct<email: large_string>")
    print("GeoLocation: struct<latitude: double, longitude: double, altitude: double>")

    print("\n" + "=" * 60)
    print("Clean user queries enabled:")
    print("SELECT file_info.path FROM my_table")
    print("SELECT location.latitude, location.longitude FROM my_table")
    print("SELECT user_id.uuid FROM my_table")
