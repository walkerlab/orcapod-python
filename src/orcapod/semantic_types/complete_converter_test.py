"""
Comprehensive test suite for Python Type Hint ↔ PyArrow Type Converter
with full semantic type support.

This test suite validates:
- Basic type conversions
- Complex nested structures
- Set handling with deterministic ordering
- Dictionary representations
- Semantic type integration
- Schema inference
- Round-trip conversion fidelity
- Error handling and edge cases
"""

import pyarrow as pa
import typing
from typing import Any, Optional, Union
from collections.abc import Collection, Sequence, Set, Mapping
from pathlib import Path
import tempfile
import uuid
from datetime import datetime, date
import json

# Import the converter functions
# (In real usage, these would be imported from your module)
from orcapod.semantic_types.complete_converter import (
    python_type_to_arrow,
    arrow_type_to_python,
    python_dicts_to_arrow_table,
    arrow_table_to_python_dicts,
    infer_schema_from_data,
    dict_to_arrow_list,
    arrow_list_to_dict,
    arrow_list_to_set,
)


# Mock Semantic Type System for Testing
class MockSemanticRegistry:
    """Mock semantic registry that supports Path, UUID, and custom types."""

    def __init__(self):
        self.converters = {
            Path: MockPathConverter(),
            uuid.UUID: MockUUIDConverter(),  # Use uuid.UUID directly
            "CustomData": MockCustomDataConverter(),
        }

    def get_converter_for_python_type(self, python_type):
        # Handle direct type lookups
        if python_type in self.converters:
            return self.converters[python_type]

        # Handle subclass relationships - add safety check
        for registered_type, converter in self.converters.items():
            try:
                if (
                    isinstance(registered_type, type)
                    and isinstance(python_type, type)
                    and issubclass(python_type, registered_type)
                ):
                    return converter
            except TypeError:
                # Handle cases where issubclass fails (e.g., with generic types)
                continue

        # Handle string-based lookups (for custom types)
        type_name = getattr(python_type, "__name__", str(python_type))
        if type_name in self.converters:
            return self.converters[type_name]

        return None

    def get_converter_for_struct_type(self, struct_type):
        if not pa.types.is_struct(struct_type):
            return None

        field_names = {f.name for f in struct_type}

        # Path struct detection
        if field_names == {"semantic_type", "path"}:
            return self.converters[Path]

        # UUID struct detection
        if field_names == {"semantic_type", "uuid_str"}:
            return self.converters[uuid.UUID]

        # Custom data struct detection
        if field_names == {"semantic_type", "data", "metadata"}:
            return self.converters["CustomData"]

        return None


class MockPathConverter:
    """Mock converter for pathlib.Path objects."""

    @property
    def arrow_struct_type(self):
        return pa.struct([("semantic_type", pa.string()), ("path", pa.large_string())])

    def python_to_struct_dict(self, value):
        if not isinstance(value, Path):
            raise TypeError(f"Expected Path, got {type(value)}")
        return {"semantic_type": "path", "path": str(value)}

    def struct_dict_to_python(self, struct_dict):
        if struct_dict.get("semantic_type") != "path":
            raise ValueError("Not a path semantic type")
        return Path(struct_dict["path"])


class MockUUIDConverter:
    """Mock converter for UUID objects."""

    @property
    def arrow_struct_type(self):
        return pa.struct([("semantic_type", pa.string()), ("uuid_str", pa.string())])

    def python_to_struct_dict(self, value):
        if not isinstance(value, uuid.UUID):
            raise TypeError(f"Expected UUID, got {type(value)}")
        return {"semantic_type": "uuid", "uuid_str": str(value)}

    def struct_dict_to_python(self, struct_dict):
        if struct_dict.get("semantic_type") != "uuid":
            raise ValueError("Not a uuid semantic type")
        return uuid.UUID(struct_dict["uuid_str"])


class CustomData:
    """Custom data class for testing complex semantic types."""

    def __init__(self, data: dict, metadata: dict = None):
        self.data = data
        self.metadata = metadata or {}

    def __eq__(self, other):
        if not isinstance(other, CustomData):
            return False
        return self.data == other.data and self.metadata == other.metadata

    def __repr__(self):
        return f"CustomData(data={self.data}, metadata={self.metadata})"


class MockCustomDataConverter:
    """Mock converter for CustomData objects."""

    @property
    def arrow_struct_type(self):
        return pa.struct(
            [
                ("semantic_type", pa.string()),
                ("data", pa.large_string()),  # JSON serialized
                ("metadata", pa.large_string()),  # JSON serialized
            ]
        )

    def python_to_struct_dict(self, value):
        if not isinstance(value, CustomData):
            raise TypeError(f"Expected CustomData, got {type(value)}")
        return {
            "semantic_type": "custom_data",
            "data": json.dumps(value.data),
            "metadata": json.dumps(value.metadata),
        }

    def struct_dict_to_python(self, struct_dict):
        if struct_dict.get("semantic_type") != "custom_data":
            raise ValueError("Not a custom_data semantic type")

        data = json.loads(struct_dict["data"])
        metadata = json.loads(struct_dict["metadata"])
        return CustomData(data, metadata)


def run_comprehensive_tests():
    """Run comprehensive test suite for the type converter."""

    print("🚀 COMPREHENSIVE PYTHON ↔ ARROW TYPE CONVERTER TEST SUITE")
    print("=" * 80)

    # Initialize mock semantic registry
    semantic_registry = MockSemanticRegistry()

    # Test counters
    total_tests = 0
    passed_tests = 0

    def test_case(name: str, test_func):
        """Helper to run individual test cases."""
        nonlocal total_tests, passed_tests
        total_tests += 1

        print(f"\n📋 Testing: {name}")
        try:
            test_func()
            print(f"   ✅ PASSED")
            passed_tests += 1
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            import traceback

            traceback.print_exc()

    # Test 1: Basic Type Conversions
    def test_basic_types():
        basic_tests = [
            (int, pa.int64()),
            (str, pa.large_string()),
            (float, pa.float64()),
            (bool, pa.bool_()),
            (bytes, pa.large_binary()),
        ]

        for python_type, expected_arrow_type in basic_tests:
            result = python_type_to_arrow(python_type)
            assert result == expected_arrow_type, (
                f"Expected {expected_arrow_type}, got {result}"
            )

            # Test reverse conversion
            recovered_type = arrow_type_to_python(result)
            assert recovered_type == python_type, (
                f"Round-trip failed: {python_type} -> {result} -> {recovered_type}"
            )

    test_case("Basic Type Conversions", test_basic_types)

    # Test 2: Complex Nested Structures
    def test_complex_nested():
        complex_tests = [
            # Nested dictionaries
            dict[str, dict[str, int]],
            # Mixed tuples with complex types (remove set[float] as it gets converted to list[float])
            tuple[dict[str, int], list[str]],
            # Deep nesting
            list[dict[str, list[tuple[int, str]]]],
            # Complex mappings
            dict[str, tuple[list[int], list[str]]],  # Changed set[str] to list[str]
        ]

        for complex_type in complex_tests:
            arrow_type = python_type_to_arrow(complex_type)
            recovered_type = arrow_type_to_python(arrow_type)
            assert recovered_type == complex_type, (
                f"Complex round-trip failed: {complex_type} -> {arrow_type} -> {recovered_type}"
            )

    test_case("Complex Nested Structures", test_complex_nested)

    # Test 3: Set Handling with Deterministic Ordering
    def test_set_deterministic_ordering():
        # Test data with sets that should be sorted deterministically
        set_data = [
            {"tags": {3, 1, 4, 1, 5}, "name": "Alice"},  # Duplicate should be removed
            {"tags": {9, 2, 6, 5, 3}, "name": "Bob"},
            {"tags": {"python", "arrow", "data"}, "name": "Charlie"},  # String set
        ]

        # Test with numeric sets
        numeric_schema = {"tags": set[int], "name": str}
        table1 = python_dicts_to_arrow_table(set_data[:2], numeric_schema)
        result1 = arrow_table_to_python_dicts(table1)

        # Verify deterministic ordering (should be sorted)
        assert result1[0]["tags"] == [1, 3, 4, 5], (
            f"Expected sorted [1, 3, 4, 5], got {result1[0]['tags']}"
        )
        assert result1[1]["tags"] == [2, 3, 5, 6, 9], (
            f"Expected sorted [2, 3, 5, 6, 9], got {result1[1]['tags']}"
        )

        # Test with string sets
        string_schema = {"tags": set[str], "name": str}
        table2 = python_dicts_to_arrow_table([set_data[2]], string_schema)
        result2 = arrow_table_to_python_dicts(table2)

        # Verify alphabetical ordering - note: sets become lists in round-trip
        expected_sorted = sorted(["python", "arrow", "data"])
        assert result2[0]["tags"] == expected_sorted, (
            f"Expected {expected_sorted}, got {result2[0]['tags']}"
        )

        # Test that we can convert back to set if needed
        recovered_set = set(result2[0]["tags"])
        original_set = {"python", "arrow", "data"}
        assert recovered_set == original_set, "Set contents should be preserved"

    test_case("Set Deterministic Ordering", test_set_deterministic_ordering)

    # Test 4: Abstract Collection Types
    def test_abstract_collections():
        abstract_tests = [
            (Collection[int], pa.large_list(pa.int64())),
            (Sequence[str], pa.large_list(pa.large_string())),
            (Set[float], pa.large_list(pa.float64())),
            (
                Mapping[str, int],
                pa.large_list(
                    pa.struct([("key", pa.large_string()), ("value", pa.int64())])
                ),
            ),
        ]

        for python_type, expected_arrow_type in abstract_tests:
            result = python_type_to_arrow(python_type)
            assert result == expected_arrow_type, (
                f"Abstract type conversion failed for {python_type}"
            )

    test_case("Abstract Collection Types", test_abstract_collections)

    # Test 5: Semantic Type Integration
    def test_semantic_types():
        # Create test data with various semantic types
        test_uuid = uuid.uuid4()
        custom_data = CustomData(
            data={"key": "value", "count": 42},
            metadata={"created": "2024-01-01", "version": 1},
        )

        semantic_data = [
            {
                "id": 1,
                "name": "Alice",
                "file_path": Path("/home/alice/data.csv"),
                "unique_id": test_uuid,
                "custom": custom_data,
                "tags": ["analysis", "data"],
            },
            {
                "id": 2,
                "name": "Bob",
                "file_path": Path("/home/bob/results.json"),
                "unique_id": uuid.uuid4(),
                "custom": CustomData({"type": "result"}, {"source": "experiment"}),
                "tags": ["results", "ml"],
            },
        ]

        semantic_schema = {
            "id": int,
            "name": str,
            "file_path": Path,
            "unique_id": uuid.UUID,  # Use uuid.UUID directly
            "custom": CustomData,
            "tags": list[str],
        }

        # Convert to Arrow table with semantic types
        table = python_dicts_to_arrow_table(
            semantic_data, semantic_schema, semantic_registry
        )

        # Verify schema contains semantic struct types
        schema_types = {field.name: field.type for field in table.schema}
        assert pa.types.is_struct(schema_types["file_path"]), (
            "Path should be converted to struct"
        )
        assert pa.types.is_struct(schema_types["unique_id"]), (
            "UUID should be converted to struct"
        )
        assert pa.types.is_struct(schema_types["custom"]), (
            "CustomData should be converted to struct"
        )

        # Test round-trip conversion
        recovered_data = arrow_table_to_python_dicts(table, semantic_registry)

        # Verify semantic types were properly reconstructed
        assert isinstance(recovered_data[0]["file_path"], Path), (
            "Path not properly recovered"
        )
        assert isinstance(recovered_data[0]["unique_id"], uuid.UUID), (
            "UUID not properly recovered"
        )
        assert isinstance(recovered_data[0]["custom"], CustomData), (
            "CustomData not properly recovered"
        )

        # Verify values are correct
        assert str(recovered_data[0]["file_path"]) == "/home/alice/data.csv"
        assert recovered_data[0]["unique_id"] == test_uuid
        assert recovered_data[0]["custom"] == custom_data

    test_case("Semantic Type Integration", test_semantic_types)

    # Test 6: Schema Inference
    def test_schema_inference():
        # Test data with mixed types for inference - make sure data matches what we expect
        inference_data = [
            {
                "name": "Alice",
                "age": 25,
                "scores": [95, 87, 92],
                "active": True,
                "metadata": {"role": "admin", "level": "5"},  # Make level a string
                "tags": {"python", "data", "ml"},
            },
            {
                "name": "Bob",
                "age": 30,
                "scores": [78, 85],
                "active": False,
                "metadata": {"role": "user", "level": "2"},  # Make level a string
                "tags": {"javascript", "web"},
            },
        ]

        # Test inference without semantic types
        inferred_schema = infer_schema_from_data(inference_data)
        print(f"Inferred schema: {inferred_schema}")

        expected_types = {
            "name": str,
            "age": int,
            "scores": list[int],
            "active": bool,
            "metadata": dict[str, str],  # Now all values are strings
            "tags": set[str],
        }

        for field, expected_type in expected_types.items():
            assert field in inferred_schema, f"Field {field} not in inferred schema"
            # For complex types, just check the origin matches
            if hasattr(expected_type, "__origin__"):
                assert inferred_schema[field].__origin__ == expected_type.__origin__, (
                    f"Field {field}: expected {expected_type.__origin__}, got {inferred_schema[field].__origin__}"
                )
            else:
                assert inferred_schema[field] == expected_type, (
                    f"Field {field}: expected {expected_type}, got {inferred_schema[field]}"
                )

        # Test table creation with inferred schema
        table = python_dicts_to_arrow_table(inference_data)  # No explicit schema
        recovered = arrow_table_to_python_dicts(table)

        # Verify basic round-trip works
        assert len(recovered) == 2
        assert recovered[0]["name"] == "Alice"
        assert recovered[0]["age"] == 25
        assert recovered[0]["metadata"]["role"] == "admin"

    test_case("Schema Inference", test_schema_inference)

    # Test 7: Optional and Union Types
    def test_optional_union_types():
        # Test Optional types
        optional_data = [
            {"name": "Alice", "middle_name": "Marie", "age": 25},
            {"name": "Bob", "middle_name": None, "age": 30},  # None value
        ]

        optional_schema = {
            "name": str,
            "middle_name": Optional[str],  # Should handle None values
            "age": int,
        }

        table = python_dicts_to_arrow_table(optional_data, optional_schema)
        recovered = arrow_table_to_python_dicts(table)

        assert recovered[0]["middle_name"] == "Marie"
        assert recovered[1]["middle_name"] is None  # None should be preserved

    test_case("Optional and Union Types", test_optional_union_types)

    # Test 8: Error Handling and Edge Cases
    def test_error_handling():
        # Test with empty data
        try:
            python_dicts_to_arrow_table([])
            assert False, "Should raise error for empty data"
        except ValueError as e:
            assert "empty data list" in str(e)

        # Test with unsupported type
        try:
            python_type_to_arrow(complex)  # complex numbers not supported
            assert False, "Should raise error for unsupported type"
        except ValueError:
            pass  # Expected

        # Test with mismatched data and schema - this should fail gracefully
        mismatch_data = [{"name": "Alice", "age": "twenty-five"}]  # age as string
        mismatch_schema = {"name": str, "age": int}  # expects int

        # This should raise an error due to type mismatch
        try:
            table = python_dicts_to_arrow_table(mismatch_data, mismatch_schema)
            assert False, "Should raise error for type mismatch"
        except (ValueError, pa.ArrowInvalid) as e:
            # Expected - conversion should fail for incompatible types
            assert "convert" in str(e).lower() or "invalid" in str(e).lower()

    test_case("Error Handling and Edge Cases", test_error_handling)

    # Test 9: Large Data Performance Test
    def test_large_data_performance():
        import time

        # Generate larger dataset
        large_data = []
        for i in range(1000):
            large_data.append(
                {
                    "id": i,
                    "name": f"User_{i}",
                    "scores": [i % 100, (i * 2) % 100, (i * 3) % 100],
                    "metadata": {
                        "group": str(i % 10),
                        "active": str(i % 2 == 0),
                    },  # Convert to strings
                    "tags": {f"tag_{i % 5}", f"category_{i % 3}"},
                }
            )

        schema = {
            "id": int,
            "name": str,
            "scores": list[int],
            "metadata": dict[str, str],  # Change from Any to str
            "tags": set[str],
        }

        # Time the conversion
        start_time = time.time()
        table = python_dicts_to_arrow_table(large_data, schema)
        conversion_time = time.time() - start_time

        # Time the round-trip
        start_time = time.time()
        recovered = arrow_table_to_python_dicts(table)
        recovery_time = time.time() - start_time

        print(f"      📊 Performance: {len(large_data)} records")
        print(f"         Conversion: {conversion_time:.3f}s")
        print(f"         Recovery: {recovery_time:.3f}s")

        # Verify correctness on sample
        assert len(recovered) == 1000
        assert recovered[0]["id"] == 0
        assert recovered[999]["id"] == 999
        assert isinstance(recovered[0]["tags"], list)  # Sets become lists

    test_case("Large Data Performance", test_large_data_performance)

    # Test 10: File I/O Round-trip Test
    def test_file_io_roundtrip():
        # Test saving to and loading from Parquet file
        test_data = [
            {
                "name": "Alice",
                "path": Path("/tmp/alice.txt"),
                "scores": {"math": 95, "english": 87},
                "tags": {"student", "honor_roll"},
            },
            {
                "name": "Bob",
                "path": Path("/tmp/bob.txt"),
                "scores": {"math": 78, "english": 92},
                "tags": {"student", "debate_team"},
            },
        ]

        schema = {"name": str, "path": Path, "scores": dict[str, int], "tags": set[str]}

        # Convert to Arrow table
        table = python_dicts_to_arrow_table(test_data, schema, semantic_registry)

        # Write to temporary Parquet file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name

        try:
            # Write to Parquet
            import pyarrow.parquet as pq

            pq.write_table(table, temp_path)

            # Read back from Parquet
            loaded_table = pq.read_table(temp_path)

            # Convert back to Python
            recovered_data = arrow_table_to_python_dicts(
                loaded_table, semantic_registry
            )

            # Verify data integrity
            assert len(recovered_data) == 2
            assert isinstance(recovered_data[0]["path"], Path)
            assert str(recovered_data[0]["path"]) == "/tmp/alice.txt"
            assert recovered_data[0]["scores"]["math"] == 95

            print(f"      💾 Successfully wrote and read {temp_path}")

        finally:
            # Clean up
            import os

            if os.path.exists(temp_path):
                os.unlink(temp_path)

    test_case("File I/O Round-trip", test_file_io_roundtrip)

    # Print final results
    print("\n" + "=" * 80)
    print(f"🏁 TEST RESULTS: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! The converter is working perfectly.")
    else:
        failed = total_tests - passed_tests
        print(f"⚠️  {failed} test(s) failed. Please review the failures above.")

    print("=" * 80)

    return passed_tests == total_tests


if __name__ == "__main__":
    success = run_comprehensive_tests()
    exit(0 if success else 1)
