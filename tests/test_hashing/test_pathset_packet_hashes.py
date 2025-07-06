#!/usr/bin/env python
"""
Test pathset and packet hash consistency.

This script verifies that the hash_pathset and hash_packet functions produce consistent
hash values for the sample pathsets and packets created by generate_pathset_packet_hashes.py.
"""

import json
from pathlib import Path

import pytest

# Add the parent directory to the path to import orcapod
from orcapod.hashing.legacy_core import hash_packet, hash_pathset


def load_pathset_hash_lut():
    """Load the pathset hash lookup table from the JSON file."""
    hash_lut_path = Path(__file__).parent / "hash_samples" / "pathset_hash_lut.json"

    if not hash_lut_path.exists():
        pytest.skip(
            f"Pathset hash lookup table not found at {hash_lut_path}. "
            "Run generate_pathset_packet_hashes.py first."
        )

    with open(hash_lut_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_packet_hash_lut():
    """Load the packet hash lookup table from the JSON file."""
    hash_lut_path = Path(__file__).parent / "hash_samples" / "packet_hash_lut.json"

    if not hash_lut_path.exists():
        pytest.skip(
            f"Packet hash lookup table not found at {hash_lut_path}. "
            "Run generate_pathset_packet_hashes.py first."
        )

    with open(hash_lut_path, "r", encoding="utf-8") as f:
        return json.load(f)


def verify_path_exists(rel_path):
    """Verify that the sample path exists."""
    # Convert relative path to absolute path
    path = Path(__file__).parent / rel_path
    if not path.exists():
        pytest.skip(
            f"Sample path not found: {path}. "
            "Run generate_pathset_packet_hashes.py first."
        )
    return path


def test_pathset_hash_consistency():
    """Test that hash_pathset produces consistent results for the sample pathsets."""
    hash_lut = load_pathset_hash_lut()

    for name, info in hash_lut.items():
        paths_rel = info["paths"]
        pathset_type = info["type"]
        expected_hash = info["hash"]

        # Create actual pathset based on type
        if pathset_type == "single_file":
            # Single file pathset
            path = verify_path_exists(paths_rel[0])
            actual_hash = hash_pathset(path)
        elif pathset_type == "directory":
            # Directory pathset
            path = verify_path_exists(paths_rel[0])
            actual_hash = hash_pathset(path)
        elif pathset_type == "collection":
            # Collection of paths
            paths = [verify_path_exists(p) for p in paths_rel]
            actual_hash = hash_pathset(paths)
        else:
            pytest.fail(f"Unknown pathset type: {pathset_type}")

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for pathset {name}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for pathset {name}: {actual_hash}")


def test_packet_hash_consistency():
    """Test that hash_packet produces consistent results for the sample packets."""
    hash_lut = load_packet_hash_lut()

    for name, info in hash_lut.items():
        structure = info["structure"]
        expected_hash = info["hash"]

        # Reconstruct the packet
        packet = {}
        for key, value in structure.items():
            if isinstance(value, list):
                # Collection of paths
                packet[key] = [verify_path_exists(p) for p in value]
            else:
                # Single path
                packet[key] = verify_path_exists(value)

        # Compute hash with current implementation
        actual_hash = hash_packet(packet)

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for packet {name}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for packet {name}: {actual_hash}")


def test_pathset_hash_algorithm_parameters():
    """Test that hash_pathset produces expected results with different algorithms and parameters."""
    # Use the first pathset in the lookup table for this test
    hash_lut = load_pathset_hash_lut()
    if not hash_lut:
        pytest.skip("No pathsets in hash lookup table")

    name, info = next(iter(hash_lut.items()))
    paths_rel = info["paths"]
    pathset_type = info["type"]

    # Create the pathset based on type
    if pathset_type == "single_file" or pathset_type == "directory":
        pathset = verify_path_exists(paths_rel[0])
    else:  # Collection
        pathset = [verify_path_exists(p) for p in paths_rel]

    # Test with different algorithms
    algorithms = ["sha256", "sha1", "md5", "xxh64", "crc32"]

    for algorithm in algorithms:
        try:
            hash1 = hash_pathset(pathset, algorithm=algorithm)
            hash2 = hash_pathset(pathset, algorithm=algorithm)
            assert hash1 == hash2, f"Hash inconsistent for algorithm {algorithm}"
            print(f"Verified {algorithm} hash consistency for pathset: {hash1}")
        except ValueError as e:
            print(f"Algorithm {algorithm} not supported: {e}")

    # Test with different buffer sizes
    buffer_sizes = [1024, 4096, 16384, 65536]

    for buffer_size in buffer_sizes:
        hash1 = hash_pathset(pathset, buffer_size=buffer_size)
        hash2 = hash_pathset(pathset, buffer_size=buffer_size)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")


def test_packet_hash_algorithm_parameters():
    """Test that hash_packet produces expected results with different algorithms and parameters."""
    # Use the first packet in the lookup table for this test
    hash_lut = load_packet_hash_lut()
    if not hash_lut:
        pytest.skip("No packets in hash lookup table")

    name, info = next(iter(hash_lut.items()))
    structure = info["structure"]

    # Reconstruct the packet
    packet = {}
    for key, value in structure.items():
        if isinstance(value, list):
            # Collection of paths
            packet[key] = [verify_path_exists(p) for p in value]
        else:
            # Single path
            packet[key] = verify_path_exists(value)

    # Test with different algorithms
    algorithms = ["sha256", "sha1", "md5", "xxh64", "crc32"]

    for algorithm in algorithms:
        try:
            hash1 = hash_packet(packet, algorithm=algorithm)
            hash2 = hash_packet(packet, algorithm=algorithm)
            # Extract hash part without algorithm prefix for comparison
            hash1_parts = hash1.split("-", 1)

            assert hash1_parts[0] == algorithm, (
                f"Algorithm prefix mismatch: expected {algorithm}, got {hash1_parts[0]}"
            )
            assert hash1 == hash2, f"Hash inconsistent for algorithm {algorithm}"
            print(f"Verified {algorithm} hash consistency for packet: {hash1}")
        except ValueError as e:
            print(f"Algorithm {algorithm} not supported: {e}")

    # Test with different buffer sizes
    buffer_sizes = [1024, 4096, 16384, 65536]

    for buffer_size in buffer_sizes:
        hash1 = hash_packet(packet, buffer_size=buffer_size)
        hash2 = hash_packet(packet, buffer_size=buffer_size)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")

    # Test with different char_count values
    char_counts = [8, 16, 32, 64, None]

    for char_count in char_counts:
        hash1 = hash_packet(packet, char_count=char_count, prefix_algorithm=False)
        hash2 = hash_packet(packet, char_count=char_count, prefix_algorithm=False)
        assert hash1 == hash2, f"Hash inconsistent for char_count {char_count}"

        # Verify the length of the hash if char_count is specified
        if char_count is not None:
            assert len(hash1) == char_count, (
                f"Hash length mismatch for char_count {char_count}: "
                f"expected {char_count}, got {len(hash1)}"
            )

        print(f"Verified hash consistency with char_count {char_count}: {hash1}")

    # Test with and without algorithm prefix
    hash_with_prefix = hash_packet(packet, prefix_algorithm=True)
    hash_without_prefix = hash_packet(packet, prefix_algorithm=False)

    assert "-" in hash_with_prefix, "Hash with prefix should contain a hyphen"
    assert hash_with_prefix.split("-", 1)[1] == hash_without_prefix, (
        "Hash without prefix should match the part after the hyphen in hash with prefix"
    )
    print(
        f"Verified prefix behavior: with={hash_with_prefix}, without={hash_without_prefix}"
    )


if __name__ == "__main__":
    print("Testing pathset hash consistency...")
    test_pathset_hash_consistency()

    print("\nTesting pathset hash algorithm parameters...")
    test_pathset_hash_algorithm_parameters()

    print("\nTesting packet hash consistency...")
    test_packet_hash_consistency()

    print("\nTesting packet hash algorithm parameters...")
    test_packet_hash_algorithm_parameters()

    print("\nAll tests passed!")
