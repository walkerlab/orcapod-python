import uuid
from collections import OrderedDict, namedtuple
from pathlib import Path
from typing import Any

from orcapod.hashing.legacy_core import HashableMixin, hash_to_hex, process_structure


# Define a simple HashableMixin class for testing
class SimpleHashable(HashableMixin):
    def __init__(self, value):
        self.value = value

    def identity_structure(self):
        return {"value": self.value}


# Define a class with __dict__ for testing
class SimpleObject:
    def __init__(self, a, b):
        self.a = a
        self.b = b


# Define a class without __dict__ for testing
class SlotObject:
    __slots__ = ["x", "y"]

    def __init__(self, x, y):
        self.x = x
        self.y = y


# Define a named tuple for testing
Person = namedtuple("Person", ["name", "age", "email"])


# Define a function for testing function handling
def sample_function(a, b, c=None):
    """Test function docstring."""
    return a + b + (c or 0)


def test_basic_object():
    """Test processing of basic object types."""
    assert process_structure(None) is None, "Expected None to return None"
    assert process_structure(True) is True, "Expected True to return True"
    assert process_structure(False) is False, "Expected False to return False"
    assert process_structure(42) == 42, "Expected integers to be preserved"
    assert process_structure(3.14) == 3.14, "Expected floats to be preserved"
    assert process_structure("hello") == "hello", "Expected strings to be preserved"
    assert process_structure("") == "", "Expected empty strings to be preserved"


def test_bytes_and_bytearray():
    """Test processing of bytes and bytearray objects."""
    assert process_structure(b"hello") == "68656c6c6f", (
        "Expected bytes to be converted to hex"
    )
    assert process_structure(bytearray(b"world")) == "776f726c64", (
        "Expected bytearray to be converted to hex"
    )
    assert process_structure(b"") == "", (
        "Expected empty bytes to be converted to empty string hex"
    )
    assert process_structure(b"\x00\x01\x02\x03") == "00010203", (
        "Expected binary bytes to be converted properly"
    )


def test_collections():
    """Test processing of various collection types."""
    # List processing
    assert process_structure([1, 2, 3]) == [1, 2, 3], "Expected lists to be preserved"
    assert process_structure([]) == [], "Expected empty lists to be preserved"

    # Nested list processing
    assert process_structure([1, [2, 3], 4]) == [1, [2, 3], 4], (
        "Expected nested lists to be processed correctly"
    )

    # Set processing
    set_result = process_structure({1, 2, 3})
    assert isinstance(set_result, list), "Expected sets to be converted to sorted lists"
    assert set_result == [1, 2, 3], "Expected set items to be sorted"

    # Frozenset processing
    frozenset_result = process_structure(frozenset([3, 1, 2]))
    assert isinstance(frozenset_result, list), (
        "Expected frozensets to be converted to sorted lists"
    )
    assert frozenset_result == [1, 2, 3], "Expected frozenset items to be sorted"

    # Empty set
    assert process_structure(set()) == [], (
        "Expected empty sets to be converted to empty lists"
    )


def test_dictionaries():
    """Test processing of dictionary types."""
    # Simple dict
    assert process_structure({"a": 1, "b": 2}) == {"a": 1, "b": 2}, (
        "Expected dictionaries to be preserved"
    )

    # Empty dict
    assert process_structure({}) == {}, "Expected empty dictionaries to be preserved"

    # Nested dict
    assert process_structure({"a": 1, "b": {"c": 3}}) == {"a": 1, "b": {"c": 3}}, (
        "Expected nested dicts to be processed correctly"
    )

    # Dict with non-string keys
    dict_with_nonstring_keys = process_structure({1: "a", 2: "b"})
    assert "1" in dict_with_nonstring_keys, (
        "Expected non-string keys to be converted to strings"
    )
    assert dict_with_nonstring_keys["1"] == "a", "Expected values to be preserved"

    # OrderedDict
    ordered_dict = OrderedDict([("z", 1), ("a", 2)])  # Keys not in alphabetical order
    processed_ordered_dict = process_structure(ordered_dict)
    assert isinstance(processed_ordered_dict, dict), (
        "Expected OrderedDict to be converted to dict"
    )
    assert list(processed_ordered_dict.keys()) == ["a", "z"], (
        "Expected keys to be sorted"
    )


def test_special_objects():
    """Test processing of special objects like paths and UUIDs."""
    # Path objects
    path = Path("/tmp/test")
    assert process_structure(path) == str(path), (
        "Expected Path objects to be converted to strings"
    )

    # UUID objects
    test_uuid = uuid.uuid4()
    assert process_structure(test_uuid) == str(test_uuid), (
        "Expected UUID objects to be converted to strings"
    )


def test_custom_objects():
    """Test processing of custom objects with and without __dict__."""
    # Object with __dict__
    obj = SimpleObject(1, "test")
    processed_obj = process_structure(obj)
    assert isinstance(processed_obj, str), (
        "Expected custom objects to be converted to string representations"
    )
    assert "SimpleObject" in processed_obj, (
        "Expected class name in string representation"
    )
    assert "a=int" in processed_obj, "Expected attribute type in string representation"

    # Object with __slots__
    slot_obj = SlotObject(10, 20)
    processed_slot_obj = process_structure(slot_obj)
    assert isinstance(processed_slot_obj, str), (
        "Expected slotted objects to be converted to string representations"
    )
    assert "SlotObject" in processed_slot_obj, (
        "Expected class name in string representation"
    )


def test_named_tuples():
    """Test processing of named tuples."""
    person = Person("Alice", 30, "alice@example.com")
    processed_person = process_structure(person)
    assert isinstance(processed_person, dict), (
        "Expected namedtuple to be converted to dict"
    )
    assert processed_person["name"] == "Alice", (
        "Expected namedtuple fields to be preserved"
    )
    assert processed_person["age"] == 30, "Expected namedtuple fields to be preserved"
    assert processed_person["email"] == "alice@example.com", (
        "Expected namedtuple fields to be preserved"
    )


def test_hashable_mixin():
    """Test processing of HashableMixin objects."""
    hashable = SimpleHashable("test_value")
    # HashableMixin objects should be processed by calling their content_hash method
    processed_hashable = process_structure(hashable)
    assert isinstance(processed_hashable, str), (
        "Expected HashableMixin to be converted to hash string"
    )
    assert len(processed_hashable) == 16, (
        "Expected default hash length of 16 characters"
    )
    assert processed_hashable == hashable.content_hash(), (
        "Expected processed HashableMixin to match content_hash"
    )

    # TODO: this test captures the current behavior of HashableMixin where
    # inner HashableMixin contents are processed and then hashed already
    # Consider allowing the full expansion of the structure first before hashing
    assert processed_hashable == hash_to_hex(
        process_structure({"value": "test_value"}), char_count=16
    ), "Expected HashableMixin to be processed like a dict"


def test_functions():
    """Test processing of function objects."""
    processed_func = process_structure(sample_function)
    assert isinstance(processed_func, str), (
        "Expected function to be converted to hash string"
    )


def test_nested_structures():
    """Test processing of complex nested structures."""
    complex_structure = {
        "name": "Test",
        "values": [1, 2, 3],
        "metadata": {
            "created": "2025-05-28",
            "tags": ["test", "example"],
            "settings": {
                "enabled": True,
                "limit": 100,
            },
        },
        "mixed": [1, "two", {"three": 3}, [4, 5]],
    }

    processed = process_structure(complex_structure)
    assert processed["name"] == "Test", "Expected string value to be preserved"
    assert processed["values"] == [1, 2, 3], "Expected list to be preserved"
    assert processed["metadata"]["created"] == "2025-05-28", (
        "Expected nested string to be preserved"
    )
    assert processed["metadata"]["tags"] == ["test", "example"], (
        "Expected nested list to be preserved"
    )
    assert processed["metadata"]["settings"]["enabled"] is True, (
        "Expected nested boolean to be preserved"
    )
    assert processed["mixed"][0] == 1, "Expected mixed list element to be preserved"
    assert processed["mixed"][1] == "two", "Expected mixed list element to be preserved"
    assert processed["mixed"][2]["three"] == 3, (
        "Expected nested dict in list to be preserved"
    )


def test_circular_references():
    """Test handling of circular references."""
    # Create a circular reference with a list
    circular_list: Any = [1, 2, 3]
    circular_list.append([4, 5])  # Add a regular list first
    circular_list[3].append(circular_list)  # Now create a circular reference

    processed_list = process_structure(circular_list)
    assert processed_list[0] == 1, "Expected list elements to be preserved"
    assert processed_list[3][0] == 4, "Expected nested list elements to be preserved"
    assert processed_list[3][2] == "CircularRef", (
        "Expected circular reference to be detected and marked"
    )

    # Create a circular reference with a dict
    circular_dict: Any = {"a": 1, "b": 2}
    nested_dict: Any = {"c": 3, "d": 4}
    circular_dict["nested"] = nested_dict
    nested_dict["parent"] = circular_dict  # Create circular reference

    processed_dict = process_structure(circular_dict)
    assert processed_dict["a"] == 1, "Expected dict elements to be preserved"
    assert processed_dict["nested"]["c"] == 3, (
        "Expected nested dict elements to be preserved"
    )
    assert processed_dict["nested"]["parent"] == "CircularRef", (
        "Expected circular reference to be detected and marked"
    )
