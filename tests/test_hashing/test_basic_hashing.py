from orcapod.hashing.legacy_core import (
    HashableMixin,
    hash_to_hex,
    hash_to_int,
    hash_to_uuid,
    stable_hash,
)


def test_hash_to_hex():
    # Test with string
    # Should be equivalent to hashing b'"test"'
    assert (
        hash_to_hex("test", None)
        == "4d967a30111bf29f0eba01c448b375c1629b2fed01cdfcc3aed91f1b57d5dd5e"
    )

    # Test with integer
    # Should be equivalent to hashing b'42'
    assert (
        hash_to_hex(42, None)
        == "73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049"
    )

    assert (
        hash_to_hex(True, None)
        == "b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b"
    )

    assert (
        hash_to_hex(0.256, None)
        == "79308bed382bc45abbb1297149dda93e29d676aff0b366bc5f2bb932a4ff55ca"
    )

    # equivalent to hashing b'null'
    assert (
        hash_to_hex(None, None)
        == "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b"
    )

    # Hash structure
    assert (
        hash_to_hex(["a", "b", "c"], None)
        == "fa1844c2988ad15ab7b49e0ece09684500fad94df916859fb9a43ff85f5bb477"
    )

    # hash set
    assert (
        hash_to_hex(set([1, 2, 3]), None)
        == "a615eeaee21de5179de080de8c3052c8da901138406ba71c38c032845f7d54f4"
    )

    # Test with custom char_count
    assert len(hash_to_hex("test", char_count=16)) == 16

    assert len(hash_to_hex("test", char_count=0)) == 0


def test_structure_equivalence():
    # identical content should yield the same hash
    assert hash_to_hex(["a", "b", "c"], None) == hash_to_hex(["a", "b", "c"], None)
    # list should be order dependent
    assert hash_to_hex(["a", "b", "c"], None) != hash_to_hex(["a", "c", "b"], None)

    # dict should be order independent
    assert hash_to_hex({"a": 1, "b": 2, "c": 3}, None) == hash_to_hex(
        {"c": 3, "b": 2, "a": 1}, None
    )

    # set should be order independent
    assert hash_to_hex(set([1, 2, 3]), None) == hash_to_hex(set([3, 2, 1]), None)

    # equivalence under nested structure
    assert hash_to_hex(set([("a", "b", "c"), ("d", "e", "f")]), None) == hash_to_hex(
        set([("d", "e", "f"), ("a", "b", "c")]), None
    )


def test_hash_to_int():
    # Test with string
    assert isinstance(hash_to_int("test"), int)

    # Test with custom hexdigits
    result = hash_to_int("test", hexdigits=8)
    assert result < 16**8  # Should be less than max value for 8 hex digits


def test_hash_to_uuid():
    # Test with string
    uuid = hash_to_uuid("test")
    assert str(uuid).count("-") == 4  # Valid UUID format

    # Test with integer
    uuid = hash_to_uuid(42)
    assert str(uuid).count("-") == 4  # Valid UUID format


class ExampleHashableMixin(HashableMixin):
    def __init__(self, value):
        self.value = value

    def identity_structure(self):
        return {"value": self.value}


def test_hashable_mixin():
    # Test that it returns a UUID
    example = ExampleHashableMixin("test")
    uuid = example.content_hash_uuid()
    assert str(uuid).count("-") == 4  # Valid UUID format

    value = example.content_hash_int()
    assert isinstance(value, int)

    # Test that it returns the same UUID for the same value
    example2 = ExampleHashableMixin("test")
    assert example.content_hash() == example2.content_hash()

    # Test that it returns different UUIDs for different values
    example3 = ExampleHashableMixin("different")
    assert example.content_hash() != example3.content_hash()


def test_stable_hash():
    # Test that same input gives same output
    assert stable_hash("test") == stable_hash("test")

    # Test that different inputs give different outputs
    assert stable_hash("test1") != stable_hash("test2")

    # Test with different types
    assert isinstance(stable_hash(42), int)
    assert isinstance(stable_hash("string"), int)
    assert isinstance(stable_hash([1, 2, 3]), int)
