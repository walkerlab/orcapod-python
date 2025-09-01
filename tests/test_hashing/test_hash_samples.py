"""
Tests for hash samples consistency.

This script tests that the hash functions produce the same outputs for
the same inputs as recorded in the samples files. This helps ensure that
the hashing implementation remains stable over time.
"""

import json
import os
from pathlib import Path

import pytest

from orcapod.hashing.legacy_core import hash_to_hex, hash_to_int, hash_to_uuid


def get_latest_hash_samples():
    """Get the path to the latest hash samples file."""
    samples_dir = Path(__file__).parent / "hash_samples" / "data_structures"
    print(f"Looking for hash samples in {samples_dir}")
    sample_files = list(samples_dir.glob("hash_examples_*.json"))
    print(f"Found {len(sample_files)} sample files")

    if not sample_files:
        print(f"No hash sample files found in {samples_dir}")
        pytest.skip("No hash sample files found")
        return None

    # Sort by modification time (newest first)
    sample_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Return the newest file
    latest = sample_files[0]
    print(f"Using latest sample file: {latest}")
    return latest


def load_hash_samples(file_path=None):
    """Load hash samples from a file or the latest file if not specified."""
    if file_path is None:
        file_path = get_latest_hash_samples()

    with open(file_path, "r") as f:
        return json.load(f)


def deserialize_value(serialized_value):
    """Convert serialized values back to their original form."""
    if isinstance(serialized_value, str) and serialized_value.startswith("bytes:"):
        # Convert hex string back to bytes
        hex_str = serialized_value[len("bytes:") :]
        return bytes.fromhex(hex_str)

    if isinstance(serialized_value, str) and serialized_value.startswith("set:"):
        # Convert string representation back to set
        # Example: "set:[1, 2, 3]" -> {1, 2, 3}
        set_str = serialized_value[len("set:") :]
        # This is a simplified approach; for a real implementation you might want to use ast.literal_eval
        # but for our test cases, we can just handle the basic cases
        if set_str == "[]":
            return set()
        elif set_str.startswith("[") and set_str.endswith("]"):
            # Parse items inside the brackets
            items_str = set_str[1:-1]
            if not items_str:
                return set()

            items = []
            for item_str in items_str.split(", "):
                item_str = item_str.strip()
                if item_str.startswith("'") and item_str.endswith("'"):
                    # It's a string
                    items.append(item_str[1:-1])
                elif item_str.lower() == "true":
                    items.append(True)
                elif item_str.lower() == "false":
                    items.append(False)
                elif item_str == "null":
                    items.append(None)
                else:
                    try:
                        # Try to parse as a number
                        if "." in item_str:
                            items.append(float(item_str))
                        else:
                            items.append(int(item_str))
                    except ValueError:
                        # If all else fails, keep it as a string
                        items.append(item_str)

            return set(items)

    return serialized_value


def test_hash_to_hex_consistency():
    """Test that hash_to_hex produces consistent results."""
    hash_samples = load_hash_samples()

    for sample in hash_samples:
        value = deserialize_value(sample["value"])
        expected_hash = sample["hex_hash"]

        # Compute the hash with the current implementation
        actual_hash = hash_to_hex(value)

        # Verify the hash matches the stored value
        assert actual_hash == expected_hash, (
            f"Hash mismatch for {sample['value']}: expected {expected_hash}, got {actual_hash}"
        )


def test_hash_to_int_consistency():
    """Test that hash_to_int produces consistent results."""
    hash_samples = load_hash_samples()

    for sample in hash_samples:
        value = deserialize_value(sample["value"])
        expected_hash = sample["int_hash"]

        # Compute the hash with the current implementation
        actual_hash = hash_to_int(value)

        # Verify the hash matches the stored value
        assert actual_hash == expected_hash, (
            f"Hash mismatch for {sample['value']}: expected {expected_hash}, got {actual_hash}"
        )


def test_hash_to_uuid_consistency():
    """Test that hash_to_uuid produces consistent results."""
    hash_samples = load_hash_samples()

    for sample in hash_samples:
        value = deserialize_value(sample["value"])
        expected_hash = sample["uuid_hash"]

        # Compute the hash with the current implementation
        actual_hash = str(hash_to_uuid(value))

        # Verify the hash matches the stored value
        assert actual_hash == expected_hash, (
            f"Hash mismatch for {sample['value']}: expected {expected_hash}, got {actual_hash}"
        )


if __name__ == "__main__":
    # This allows running the tests directly for debugging
    samples = load_hash_samples()
    print(f"Loaded {len(samples)} hash samples")

    print("\nTesting hash_to_hex consistency...")
    test_hash_to_hex_consistency()

    print("\nTesting hash_to_int consistency...")
    test_hash_to_int_consistency()

    print("\nTesting hash_to_uuid consistency...")
    test_hash_to_uuid_consistency()

    print("\nAll tests passed!")
