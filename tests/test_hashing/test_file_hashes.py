#!/usr/bin/env python
"""
Test file hash consistency.

This script verifies that the hash_file function produces consistent
hash values for the sample files created by generate_file_hashes.py.
"""

import json
from pathlib import Path

import pytest

# Add the parent directory to the path to import orcapod
from orcapod.hashing.legacy_core import hash_file


def load_hash_lut():
    """Load the hash lookup table from the JSON file."""
    hash_lut_path = Path(__file__).parent / "hash_samples" / "file_hash_lut.json"

    if not hash_lut_path.exists():
        pytest.skip(
            f"Hash lookup table not found at {hash_lut_path}. Run generate_file_hashes.py first."
        )

    with open(hash_lut_path, "r", encoding="utf-8") as f:
        return json.load(f)


def verify_file_exists(rel_path):
    """Verify that the sample file exists."""
    # Convert relative path to absolute path
    file_path = Path(__file__).parent / rel_path
    if not file_path.exists():
        pytest.skip(
            f"Sample file not found: {file_path}. Run generate_file_hashes.py first."
        )
    return file_path


def test_file_hash_consistency():
    """Test that hash_file produces consistent results for the sample files."""
    hash_lut = load_hash_lut()

    for filename, info in hash_lut.items():
        rel_path = info["file"]
        expected_hash = info["hash"]

        # Verify file exists and get absolute path
        file_path = verify_file_exists(rel_path)

        # Compute hash with current implementation
        actual_hash = hash_file(file_path)

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for {filename}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for {filename}: {actual_hash}")


def test_file_hash_algorithm_parameters():
    """Test that hash_file produces expected results with different algorithms and parameters."""
    # Use the first file in the hash lookup table for this test
    hash_lut = load_hash_lut()
    if not hash_lut:
        pytest.skip("No files in hash lookup table")

    filename, info = next(iter(hash_lut.items()))
    rel_path = info["file"]

    # Get absolute path to the file
    file_path = verify_file_exists(rel_path)

    # Test with different algorithms
    algorithms = ["sha256", "sha1", "md5", "xxh64", "crc32"]

    for algorithm in algorithms:
        try:
            hash1 = hash_file(file_path, algorithm=algorithm)
            hash2 = hash_file(file_path, algorithm=algorithm)
            assert hash1 == hash2, f"Hash inconsistent for algorithm {algorithm}"
            print(f"Verified {algorithm} hash consistency: {hash1}")
        except ValueError as e:
            print(f"Algorithm {algorithm} not supported: {e}")

    # Test with different buffer sizes
    buffer_sizes = [1024, 4096, 16384, 65536]

    for buffer_size in buffer_sizes:
        hash1 = hash_file(file_path, buffer_size=buffer_size)
        hash2 = hash_file(file_path, buffer_size=buffer_size)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")


if __name__ == "__main__":
    print("Testing file hash consistency...")
    test_file_hash_consistency()

    print("\nTesting file hash algorithm parameters...")
    test_file_hash_algorithm_parameters()

    print("\nAll tests passed!")
