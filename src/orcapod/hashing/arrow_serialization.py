import pyarrow as pa
from io import BytesIO
import pyarrow.ipc as ipc
import struct
from typing import Any
import hashlib


def serialize_table_ipc(table: pa.Table) -> bytes:
    # TODO: fix and use logical table hashing instead
    """Serialize table using Arrow IPC format for stable binary representation."""
    buffer = BytesIO()

    # Write format version
    buffer.write(b"ARROW_IPC_V1")

    # Use IPC stream format for deterministic serialization
    with ipc.new_stream(buffer, table.schema) as writer:
        writer.write_table(table)

    return buffer.getvalue()


def serialize_table_logical(table: pa.Table) -> bytes:
    """
    Serialize table using column-wise processing with direct binary data access.

    This implementation works directly with Arrow's underlying binary buffers
    without converting to Python objects, making it much faster and more
    memory efficient while maintaining high repeatability.
    """
    buffer = BytesIO()

    # Write format version
    buffer.write(b"ARROW_BINARY_V1")

    # Serialize schema deterministically
    _serialize_schema_deterministic(buffer, table.schema)

    # Process each column using direct binary access
    column_digests = []
    for i in range(table.num_columns):
        column = table.column(i)
        field = table.schema.field(i)
        column_digest = _serialize_column_binary(column, field)
        column_digests.append(column_digest)

    # Combine column digests
    for digest in column_digests:
        buffer.write(digest)

    return buffer.getvalue()


def _serialize_schema_deterministic(buffer: BytesIO, schema: pa.Schema) -> None:
    """Serialize schema information deterministically."""
    buffer.write(struct.pack("<Q", len(schema)))

    for field in schema:
        # Field name as UTF-8
        name_bytes = field.name.encode("utf-8")
        buffer.write(struct.pack("<Q", len(name_bytes)))
        buffer.write(name_bytes)

        # Nullable flag
        buffer.write(struct.pack("<?", field.nullable))

        # Serialize data type deterministically
        _serialize_datatype_deterministic(buffer, field.type)


def _serialize_datatype_deterministic(buffer: BytesIO, data_type: pa.DataType) -> None:
    """Serialize Arrow data type deterministically."""
    type_id = data_type.id
    buffer.write(struct.pack("<i", type_id))

    if pa.types.is_integer(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))
        buffer.write(struct.pack("<?", pa.types.is_signed_integer(data_type)))

    elif pa.types.is_floating(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))

    elif pa.types.is_decimal(data_type):
        buffer.write(struct.pack("<i", data_type.bit_width))
        buffer.write(struct.pack("<i", data_type.precision))
        buffer.write(struct.pack("<i", data_type.scale))

    elif pa.types.is_timestamp(data_type):
        unit_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))
        if data_type.tz:
            tz_bytes = data_type.tz.encode("utf-8")
            buffer.write(struct.pack("<Q", len(tz_bytes)))
            buffer.write(tz_bytes)
        else:
            buffer.write(struct.pack("<Q", 0))

    elif pa.types.is_date(data_type):
        unit_map = {"day": 0, "ms": 1}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))

    elif pa.types.is_time(data_type):
        unit_map = {"s": 0, "ms": 1, "us": 2, "ns": 3}
        buffer.write(struct.pack("<i", unit_map.get(data_type.unit, 0)))
        buffer.write(struct.pack("<i", data_type.bit_width))

    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        _serialize_datatype_deterministic(buffer, data_type.value_type)

    elif pa.types.is_struct(data_type):
        buffer.write(struct.pack("<i", len(data_type)))
        for field in data_type:
            field_name_bytes = field.name.encode("utf-8")
            buffer.write(struct.pack("<Q", len(field_name_bytes)))
            buffer.write(field_name_bytes)
            _serialize_datatype_deterministic(buffer, field.type)

    elif pa.types.is_dictionary(data_type):
        _serialize_datatype_deterministic(buffer, data_type.index_type)
        _serialize_datatype_deterministic(buffer, data_type.value_type)


def _serialize_column_binary(column: pa.ChunkedArray, field: pa.Field) -> bytes:
    """
    Serialize column using direct binary buffer access.

    To ensure chunking independence, we combine chunks into a single array
    before processing. This ensures identical output regardless of chunk boundaries.
    """
    buffer = BytesIO()

    # Combine all chunks into a single array for consistent processing
    if column.num_chunks > 1:
        # Multiple chunks - combine them
        combined_array = pa.concat_arrays(column.chunks)
    elif column.num_chunks == 1:
        # Single chunk - use directly
        combined_array = column.chunk(0)
    else:
        # No chunks - create empty array
        combined_array = pa.array([], type=field.type)

    # Process the combined array
    chunk_result = _serialize_array_binary(combined_array, field.type)
    buffer.write(chunk_result)

    return buffer.getvalue()


def _serialize_array_binary(array: pa.Array, data_type: pa.DataType) -> bytes:
    """Serialize array using direct access to Arrow's binary buffers."""
    buffer = BytesIO()

    # Get validity buffer (null bitmap) if it exists
    validity_buffer = None
    if array.buffers()[0] is not None:
        validity_buffer = array.buffers()[0]

    # Process based on Arrow type, accessing buffers directly
    if _is_primitive_type(data_type):
        _serialize_primitive_array_binary(buffer, array, data_type, validity_buffer)

    elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        _serialize_string_array_binary(buffer, array, data_type, validity_buffer)

    elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        _serialize_binary_array_binary(buffer, array, data_type, validity_buffer)

    elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        _serialize_list_array_binary(buffer, array, data_type, validity_buffer)

    elif pa.types.is_struct(data_type):
        _serialize_struct_array_binary(buffer, array, data_type, validity_buffer)

    elif pa.types.is_dictionary(data_type):
        _serialize_dictionary_array_binary(buffer, array, data_type, validity_buffer)

    else:
        # Fallback to element-wise processing for complex types
        _serialize_array_fallback(buffer, array, data_type, validity_buffer)

    return buffer.getvalue()


def _is_primitive_type(data_type: pa.DataType) -> bool:
    """Check if type can be processed as primitive (fixed-size) data."""
    return (
        pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_date(data_type)
        or pa.types.is_time(data_type)
        or pa.types.is_timestamp(data_type)
    )


def _serialize_primitive_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize primitive arrays by directly copying binary data."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Get data buffer (buffer[1] for primitive types)
    data_buffer = array.buffers()[1]
    if data_buffer is not None:
        # For primitive types, copy the buffer directly
        if pa.types.is_boolean(data_type):
            # Boolean needs the length for bit interpretation
            buffer.write(struct.pack("<Q", len(array)))
        buffer.write(data_buffer.to_pybytes())


def _serialize_string_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize string arrays using offset + data buffers."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Get offset and data buffers
    offset_buffer = array.buffers()[1]  # int32 or int64 offsets
    data_buffer = array.buffers()[2]  # UTF-8 string data

    # Write offset buffer
    if offset_buffer is not None:
        buffer.write(offset_buffer.to_pybytes())

    # Write data buffer
    if data_buffer is not None:
        buffer.write(data_buffer.to_pybytes())


def _serialize_binary_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize binary arrays using offset + data buffers."""
    # Same structure as string arrays
    _serialize_string_array_binary(buffer, array, data_type, validity_buffer)


def _serialize_list_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize list arrays using offset buffer + child array."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Write offset buffer
    offset_buffer = array.buffers()[1]
    if offset_buffer is not None:
        buffer.write(offset_buffer.to_pybytes())

    # Get child array - handle both .children and .values access patterns
    child_array = None
    if hasattr(array, "values") and array.values is not None:
        child_array = array.values
    elif hasattr(array, "children") and len(array.children) > 0:
        child_array = array.children[0]

    # Recursively serialize child array
    if child_array is not None:
        child_data = _serialize_array_binary(child_array, data_type.value_type)
        buffer.write(child_data)


def _serialize_struct_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize struct arrays by processing child arrays."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Serialize each child field
    for i, child_array in enumerate(array.children):
        field_type = data_type[i].type
        child_data = _serialize_array_binary(child_array, field_type)
        buffer.write(child_data)


def _serialize_dictionary_array_binary(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Serialize dictionary arrays using indices + dictionary."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Serialize indices array
    indices_data = _serialize_array_binary(array.indices, data_type.index_type)
    buffer.write(indices_data)

    # Serialize dictionary array
    dict_data = _serialize_array_binary(array.dictionary, data_type.value_type)
    buffer.write(dict_data)


def _serialize_validity_buffer(buffer: BytesIO, validity_buffer):
    """Serialize validity (null) bitmap."""
    if validity_buffer is not None:
        # Copy validity bitmap directly
        buffer.write(validity_buffer.to_pybytes())
    # If no validity buffer, there are no nulls (implicit)


def _serialize_boolean_buffer(buffer: BytesIO, data_buffer, array_length: int):
    """Serialize boolean buffer (bit-packed)."""
    # Boolean data is bit-packed, copy directly
    bool_bytes = data_buffer.to_pybytes()
    buffer.write(struct.pack("<Q", len(bool_bytes)))
    buffer.write(bool_bytes)
    # Also store the actual length for proper bit interpretation
    buffer.write(struct.pack("<Q", array_length))


def _get_type_byte_width(data_type: pa.DataType) -> int:
    """Get byte width of primitive types."""
    if pa.types.is_boolean(data_type):
        return 1  # Bit-packed, but minimum 1 byte
    elif pa.types.is_integer(data_type) or pa.types.is_floating(data_type):
        return data_type.bit_width // 8
    elif pa.types.is_date(data_type):
        return 4 if data_type == pa.date32() else 8
    elif pa.types.is_time(data_type) or pa.types.is_timestamp(data_type):
        return data_type.bit_width // 8
    else:
        return 8  # Default


def _serialize_array_fallback(
    buffer: BytesIO, array: pa.Array, data_type: pa.DataType, validity_buffer
):
    """Fallback to element-wise processing for complex types."""
    # Write validity bitmap
    _serialize_validity_buffer(buffer, validity_buffer)

    # Process element by element (only for types that need it)
    for i in range(len(array)):
        if array.is_null(i):
            buffer.write(b"\x00")
        else:
            buffer.write(b"\x01")
            # For complex nested types, we might still need .as_py()
            # But this should be rare with proper binary handling above
            value = array[i].as_py()
            _serialize_complex_value(buffer, value, data_type)


def _serialize_complex_value(buffer: BytesIO, value: Any, data_type: pa.DataType):
    """Serialize complex values that can't be handled by direct buffer access."""
    # This handles edge cases like nested structs with mixed types
    if pa.types.is_decimal(data_type):
        decimal_str = str(value).encode("utf-8")
        buffer.write(struct.pack("<Q", len(decimal_str)))
        buffer.write(decimal_str)
    else:
        # Generic fallback
        str_repr = str(value).encode("utf-8")
        buffer.write(struct.pack("<Q", len(str_repr)))
        buffer.write(str_repr)


def serialize_table_logical_hash(table: pa.Table, algorithm: str = "sha256") -> str:
    """Create deterministic hash using binary serialization."""
    serialized = serialize_table_logical(table)

    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "sha3_256":
        hasher = hashlib.sha3_256()
    elif algorithm == "blake2b":
        hasher = hashlib.blake2b()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    hasher.update(serialized)
    return hasher.hexdigest()


def serialize_table_logical_streaming(table: pa.Table) -> str:
    """
    Memory-efficient streaming version that produces the same hash as serialize_table_logical_hash.

    This version processes data in streaming fashion but maintains the same logical structure
    as the non-streaming version to ensure identical hashes and chunking independence.
    """
    hasher = hashlib.sha256()

    # Hash format version (same as non-streaming)
    hasher.update(b"ARROW_BINARY_V1")

    # Hash schema (same as non-streaming)
    schema_buffer = BytesIO()
    _serialize_schema_deterministic(schema_buffer, table.schema)
    hasher.update(schema_buffer.getvalue())

    # Process each column using the same logic as non-streaming
    for i in range(table.num_columns):
        column = table.column(i)
        field = table.schema.field(i)

        # Use the same column serialization logic for chunking independence
        column_data = _serialize_column_binary(column, field)

        # Hash the column data
        hasher.update(column_data)

    return hasher.hexdigest()


# Test utilities
def create_test_table_1():
    """Create a basic test table with various data types."""
    return pa.table(
        {
            "int32_col": pa.array([1, 2, None, 4, 5], type=pa.int32()),
            "float64_col": pa.array([1.1, 2.2, 3.3, None, 5.5], type=pa.float64()),
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
        }
    )


def create_test_table_reordered_columns():
    """Same data as test_table_1 but with different column order."""
    return pa.table(
        {
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "int32_col": pa.array([1, 2, None, 4, 5], type=pa.int32()),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
            "float64_col": pa.array([1.1, 2.2, 3.3, None, 5.5], type=pa.float64()),
        }
    )


def create_test_table_reordered_rows():
    """Same data as test_table_1 but with different row order."""
    return pa.table(
        {
            "int32_col": pa.array([5, 4, None, 2, 1], type=pa.int32()),
            "float64_col": pa.array([5.5, None, 3.3, 2.2, 1.1], type=pa.float64()),
            "string_col": pa.array(["fast", "arrow", None, "world", "hello"]),
            "bool_col": pa.array([False, True, None, False, True]),
            "binary_col": pa.array([b"data5", b"data4", None, b"data2", b"data1"]),
        }
    )


def create_test_table_different_types():
    """Same logical data but with different Arrow types where possible."""
    return pa.table(
        {
            "int32_col": pa.array(
                [1, 2, None, 4, 5], type=pa.int64()
            ),  # int64 instead of int32
            "float64_col": pa.array(
                [1.1, 2.2, 3.3, None, 5.5], type=pa.float32()
            ),  # float32 instead of float64
            "string_col": pa.array(["hello", "world", None, "arrow", "fast"]),
            "bool_col": pa.array([True, False, None, True, False]),
            "binary_col": pa.array([b"data1", b"data2", None, b"data4", b"data5"]),
        }
    )


def create_test_table_different_chunking():
    """Same data as test_table_1 but with different chunking."""
    # Create arrays with explicit chunking
    int_chunks = [
        pa.array([1, 2], type=pa.int32()),
        pa.array([None, 4, 5], type=pa.int32()),
    ]
    float_chunks = [
        pa.array([1.1], type=pa.float64()),
        pa.array([2.2, 3.3, None, 5.5], type=pa.float64()),
    ]
    string_chunks = [pa.array(["hello", "world"]), pa.array([None, "arrow", "fast"])]
    bool_chunks = [pa.array([True, False, None]), pa.array([True, False])]
    binary_chunks = [
        pa.array([b"data1"]),
        pa.array([b"data2", None, b"data4", b"data5"]),
    ]

    return pa.table(
        {
            "int32_col": pa.chunked_array(int_chunks),
            "float64_col": pa.chunked_array(float_chunks),
            "string_col": pa.chunked_array(string_chunks),
            "bool_col": pa.chunked_array(bool_chunks),
            "binary_col": pa.chunked_array(binary_chunks),
        }
    )


def create_test_table_empty():
    """Create an empty table with same schema."""
    return pa.table(
        {
            "int32_col": pa.array([], type=pa.int32()),
            "float64_col": pa.array([], type=pa.float64()),
            "string_col": pa.array([], type=pa.string()),
            "bool_col": pa.array([], type=pa.bool_()),
            "binary_col": pa.array([], type=pa.binary()),
        }
    )


def create_test_table_all_nulls():
    """Create a table with all null values."""
    return pa.table(
        {
            "int32_col": pa.array([None, None, None], type=pa.int32()),
            "float64_col": pa.array([None, None, None], type=pa.float64()),
            "string_col": pa.array([None, None, None], type=pa.string()),
            "bool_col": pa.array([None, None, None], type=pa.bool_()),
            "binary_col": pa.array([None, None, None], type=pa.binary()),
        }
    )


def create_test_table_no_nulls():
    """Create a table with no null values."""
    return pa.table(
        {
            "int32_col": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            "float64_col": pa.array([1.1, 2.2, 3.3, 4.4, 5.5], type=pa.float64()),
            "string_col": pa.array(["hello", "world", "arrow", "fast", "data"]),
            "bool_col": pa.array([True, False, True, False, True]),
            "binary_col": pa.array([b"data1", b"data2", b"data3", b"data4", b"data5"]),
        }
    )


def create_test_table_complex_types():
    """Create a table with complex nested types."""
    return pa.table(
        {
            "list_col": pa.array(
                [[1, 2], [3, 4, 5], None, [], [6]], type=pa.list_(pa.int32())
            ),
            "struct_col": pa.array(
                [
                    {"a": 1, "b": "x"},
                    {"a": 2, "b": "y"},
                    None,
                    {"a": 3, "b": "z"},
                    {"a": 4, "b": "w"},
                ],
                type=pa.struct([("a", pa.int32()), ("b", pa.string())]),
            ),
            "dict_col": pa.array(
                ["apple", "banana", "apple", None, "cherry"]
            ).dictionary_encode(),
        }
    )


def create_test_table_single_column():
    """Create a table with just one column."""
    return pa.table({"single_col": pa.array([1, 2, 3, 4, 5], type=pa.int32())})


def create_test_table_single_row():
    """Create a table with just one row."""
    return pa.table(
        {
            "int32_col": pa.array([42], type=pa.int32()),
            "string_col": pa.array(["single"]),
            "bool_col": pa.array([True]),
        }
    )


def run_comprehensive_tests():
    """Run comprehensive test suite for serialization."""
    import time

    print("=" * 60)
    print("COMPREHENSIVE ARROW SERIALIZATION TEST SUITE")
    print("=" * 60)

    # Test cases
    test_cases = [
        ("Basic table", create_test_table_1),
        ("Reordered columns", create_test_table_reordered_columns),
        ("Reordered rows", create_test_table_reordered_rows),
        ("Different types", create_test_table_different_types),
        ("Different chunking", create_test_table_different_chunking),
        ("Empty table", create_test_table_empty),
        ("All nulls", create_test_table_all_nulls),
        ("No nulls", create_test_table_no_nulls),
        ("Complex types", create_test_table_complex_types),
        ("Single column", create_test_table_single_column),
        ("Single row", create_test_table_single_row),
    ]

    # Generate hashes for all test cases
    results = {}

    print("\n1. GENERATING HASHES FOR ALL TEST CASES")
    print("-" * 50)

    for name, create_func in test_cases:
        try:
            table = create_func()

            # Generate all hash types
            logical_hash = serialize_table_logical_hash(table)
            streaming_hash = serialize_table_logical_streaming(table)
            ipc_hash = hashlib.sha256(serialize_table_ipc(table)).hexdigest()

            results[name] = {
                "table": table,
                "logical": logical_hash,
                "streaming": streaming_hash,
                "ipc": ipc_hash,
                "rows": table.num_rows,
                "cols": table.num_columns,
            }

            print(
                f"{name:20} | Rows: {table.num_rows:5} | Cols: {table.num_columns:2} | "
                f"Logical: {logical_hash[:12]}... | IPC: {ipc_hash[:12]}..."
            )

        except Exception as e:
            print(f"{name:20} | ERROR: {str(e)}")
            results[name] = {"error": str(e)}

    print("\n2. DETERMINISM TESTS")
    print("-" * 50)

    base_table = create_test_table_1()

    # Test multiple runs of same table
    logical_hashes = [serialize_table_logical_hash(base_table) for _ in range(5)]
    streaming_hashes = [serialize_table_logical_streaming(base_table) for _ in range(5)]
    ipc_hashes = [
        hashlib.sha256(serialize_table_ipc(base_table)).hexdigest() for _ in range(5)
    ]

    print(
        f"Logical deterministic:   {len(set(logical_hashes)) == 1} ({len(set(logical_hashes))}/5 unique)"
    )
    print(
        f"Streaming deterministic: {len(set(streaming_hashes)) == 1} ({len(set(streaming_hashes))}/5 unique)"
    )
    print(
        f"IPC deterministic:       {len(set(ipc_hashes)) == 1} ({len(set(ipc_hashes))}/5 unique)"
    )
    print(f"Streaming == Logical:    {streaming_hashes[0] == logical_hashes[0]}")

    print("\n3. EQUIVALENCE TESTS")
    print("-" * 50)

    base_logical = results["Basic table"]["logical"]
    base_ipc = results["Basic table"]["ipc"]

    equivalence_tests = [
        (
            "Same table vs reordered columns",
            "Reordered columns",
            False,
            "Different column order should produce different hash",
        ),
        (
            "Same table vs reordered rows",
            "Reordered rows",
            False,
            "Different row order should produce different hash",
        ),
        (
            "Same table vs different types",
            "Different types",
            False,
            "Different data types should produce different hash",
        ),
        (
            "Same table vs different chunking",
            "Different chunking",
            True,
            "Same data with different chunking should produce same hash",
        ),
        (
            "Same table vs no nulls",
            "No nulls",
            False,
            "Different null patterns should produce different hash",
        ),
        (
            "Same table vs all nulls",
            "All nulls",
            False,
            "Different data should produce different hash",
        ),
    ]

    for test_name, compare_case, should_match, explanation in equivalence_tests:
        if compare_case in results and "logical" in results[compare_case]:
            compare_logical = results[compare_case]["logical"]
            compare_ipc = results[compare_case]["ipc"]

            logical_match = base_logical == compare_logical
            ipc_match = base_ipc == compare_ipc

            logical_status = "✓" if logical_match == should_match else "✗"
            ipc_status = "✓" if ipc_match == should_match else "✗"

            print(f"{logical_status} {test_name}")
            print(f"   Logical: {logical_match} (expected: {should_match})")
            print(f"   IPC:     {ipc_match} (expected: {should_match})")
            print(f"   Reason:  {explanation}")
            print()

    print("4. CHUNKING INDEPENDENCE DETAILED TEST")
    print("-" * 50)

    # Test various chunking strategies
    original_table = create_test_table_1()
    combined_table = original_table.combine_chunks()
    different_chunking = create_test_table_different_chunking()

    orig_logical = serialize_table_logical_hash(original_table)
    comb_logical = serialize_table_logical_hash(combined_table)
    diff_logical = serialize_table_logical_hash(different_chunking)

    orig_ipc = hashlib.sha256(serialize_table_ipc(original_table)).hexdigest()
    comb_ipc = hashlib.sha256(serialize_table_ipc(combined_table)).hexdigest()
    diff_ipc = hashlib.sha256(serialize_table_ipc(different_chunking)).hexdigest()

    print(f"Original chunking:     {orig_logical[:16]}...")
    print(f"Combined chunks:       {comb_logical[:16]}...")
    print(f"Different chunking:    {diff_logical[:16]}...")
    print(
        f"Logical chunking-independent: {orig_logical == comb_logical == diff_logical}"
    )
    print()
    print(f"Original IPC:          {orig_ipc[:16]}...")
    print(f"Combined IPC:          {comb_ipc[:16]}...")
    print(f"Different IPC:         {diff_ipc[:16]}...")
    print(f"IPC chunking-independent:     {orig_ipc == comb_ipc == diff_ipc}")

    print("\n5. PERFORMANCE COMPARISON")
    print("-" * 50)

    # Create larger table for performance testing
    large_size = 10000
    large_table = pa.table(
        {
            "int_col": pa.array(list(range(large_size)), type=pa.int32()),
            "float_col": pa.array(
                [i * 1.5 for i in range(large_size)], type=pa.float64()
            ),
            "string_col": pa.array([f"item_{i}" for i in range(large_size)]),
            "bool_col": pa.array([i % 2 == 0 for i in range(large_size)]),
        }
    )

    # Time each method
    methods = [
        ("Logical", lambda t: serialize_table_logical_hash(t)),
        ("Streaming", lambda t: serialize_table_logical_streaming(t)),
        ("IPC", lambda t: hashlib.sha256(serialize_table_ipc(t)).hexdigest()),
    ]
    hash_result = ""
    for method_name, method_func in methods:
        times = []
        for _ in range(3):  # Run 3 times for average
            start = time.time()
            hash_result = method_func(large_table)
            end = time.time()
            times.append(end - start)

        avg_time = sum(times) / len(times)
        throughput = (large_size * 4) / avg_time  # 4 columns

        print(
            f"{method_name:10} | {avg_time * 1000:6.1f}ms | {throughput:8.0f} values/sec | {hash_result[:12]}..."
        )

    print("\n6. EDGE CASES")
    print("-" * 50)

    edge_cases = ["Empty table", "All nulls", "Single column", "Single row"]
    for case in edge_cases:
        if case in results and "error" not in results[case]:
            r = results[case]
            print(
                f"{case:15} | {r['rows']:3}r x {r['cols']:2}c | "
                f"L:{r['logical'][:8]}... | I:{r['ipc'][:8]}... | "
                f"Match: {r['logical'] == r['streaming']}"
            )

    print("\n7. COMPLEX TYPES TEST")
    print("-" * 50)

    if "Complex types" in results and "error" not in results["Complex types"]:
        complex_result = results["Complex types"]
        print(f"Complex types serialization successful:")
        print(f"  Logical hash:  {complex_result['logical']}")
        print(
            f"  Streaming ==:  {complex_result['logical'] == complex_result['streaming']}"
        )
        print(f"  Rows/Cols:     {complex_result['rows']}r x {complex_result['cols']}c")
    else:
        print(
            "Complex types test failed - this is expected for some complex nested types"
        )

    print(f"\n{'=' * 60}")
    print("TEST SUITE COMPLETE")
    print(f"{'=' * 60}")

    return results


# Main execution
if __name__ == "__main__":
    # Run the comprehensive test suite
    test_results = run_comprehensive_tests()
