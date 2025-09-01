#!/usr/bin/env python
"""
Test DefaultFileHasher functionality.

This script verifies that the DefaultFileHasher class produces consistent
hash values for files, pathsets, and packets, mirroring the tests for the core
hash functions.
"""

import json
from pathlib import Path

import pytest

from orcapod.hashing.file_hashers import LegacyPathLikeHasherFactory


def load_hash_lut():
    """Load the hash lookup table from the JSON file."""
    hash_lut_path = Path(__file__).parent / "hash_samples" / "file_hash_lut.json"

    if not hash_lut_path.exists():
        pytest.skip(
            f"Hash lookup table not found at {hash_lut_path}. Run generate_file_hashes.py first."
        )

    with open(hash_lut_path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def verify_file_exists(rel_path):
    """Verify that the sample file exists."""
    # Convert relative path to absolute path
    file_path = Path(__file__).parent / rel_path
    if not file_path.exists():
        pytest.skip(
            f"Sample file not found: {file_path}. Run generate_file_hashes.py first."
        )
    return file_path


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


def test_default_file_hasher_file_hash_consistency():
    """Test that DefaultFileHasher.hash_file produces consistent results for the sample files."""
    hash_lut = load_hash_lut()
    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite()

    for filename, info in hash_lut.items():
        rel_path = info["file"]
        expected_hash = info["hash"]

        # Verify file exists and get absolute path
        file_path = verify_file_exists(rel_path)

        # Compute hash with DefaultFileHasher
        actual_hash = hasher.hash_file(file_path)

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for {filename}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for {filename}: {actual_hash}")


def test_default_file_hasher_pathset_hash_consistency():
    """Test that DefaultFileHasher.hash_pathset produces consistent results for the sample pathsets."""
    hash_lut = load_pathset_hash_lut()
    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite()

    for name, info in hash_lut.items():
        paths_rel = info["paths"]
        pathset_type = info["type"]
        expected_hash = info["hash"]

        # Create actual pathset based on type
        if pathset_type == "single_file":
            # Single file pathset
            path = verify_path_exists(paths_rel[0])
            actual_hash = hasher.hash_pathset(path)
        elif pathset_type == "directory":
            # Directory pathset
            path = verify_path_exists(paths_rel[0])
            actual_hash = hasher.hash_pathset(path)
        elif pathset_type == "collection":
            # Collection of paths
            paths = [verify_path_exists(p) for p in paths_rel]
            actual_hash = hasher.hash_pathset(paths)
        else:
            pytest.fail(f"Unknown pathset type: {pathset_type}")

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for pathset {name}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for pathset {name}: {actual_hash}")


def test_default_file_hasher_packet_hash_consistency():
    """Test that DefaultFileHasher.hash_packet produces consistent results for the sample packets."""
    hash_lut = load_packet_hash_lut()
    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite()

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

        # Compute hash with DefaultFileHasher
        actual_hash = hasher.hash_packet(packet)

        # Verify hash consistency
        assert actual_hash == expected_hash, (
            f"Hash mismatch for packet {name}: expected {expected_hash}, got {actual_hash}"
        )
        print(f"Verified hash for packet {name}: {actual_hash}")


def test_default_file_hasher_file_hash_algorithm_parameters():
    """Test that DefaultFileHasher.hash_file produces expected results with different algorithms and parameters."""
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
            hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                algorithm=algorithm
            )
            hash1 = hasher.hash_file(file_path)
            hash2 = hasher.hash_file(file_path)
            assert hash1 == hash2, f"Hash inconsistent for algorithm {algorithm}"
            print(f"Verified {algorithm} hash consistency: {hash1}")
        except ValueError as e:
            print(f"Algorithm {algorithm} not supported: {e}")

    # Test with different buffer sizes
    buffer_sizes = [1024, 4096, 16384, 65536]

    for buffer_size in buffer_sizes:
        hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
            buffer_size=buffer_size
        )
        hash1 = hasher.hash_file(file_path)
        hash2 = hasher.hash_file(file_path)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")


def test_default_file_hasher_pathset_hash_algorithm_parameters():
    """Test that DefaultFileHasher.hash_pathset produces expected results with different algorithms and parameters."""
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
            hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                algorithm=algorithm
            )
            hash1 = hasher.hash_pathset(pathset)
            hash2 = hasher.hash_pathset(pathset)
            assert hash1 == hash2, f"Hash inconsistent for algorithm {algorithm}"
            print(f"Verified {algorithm} hash consistency for pathset: {hash1}")
        except ValueError as e:
            print(f"Algorithm {algorithm} not supported: {e}")

    # Test with different buffer sizes
    buffer_sizes = [1024, 4096, 16384, 65536]

    for buffer_size in buffer_sizes:
        hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
            buffer_size=buffer_size
        )
        hash1 = hasher.hash_pathset(pathset)
        hash2 = hasher.hash_pathset(pathset)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")


def test_default_file_hasher_packet_hash_algorithm_parameters():
    """Test that DefaultFileHasher.hash_packet produces expected results with different algorithms and parameters."""
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
            hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                algorithm=algorithm
            )
            hash1 = hasher.hash_packet(packet)
            hash2 = hasher.hash_packet(packet)

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
        hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
            buffer_size=buffer_size
        )
        hash1 = hasher.hash_packet(packet)
        hash2 = hasher.hash_packet(packet)
        assert hash1 == hash2, f"Hash inconsistent for buffer size {buffer_size}"
        print(f"Verified hash consistency with buffer size {buffer_size}: {hash1}")


if __name__ == "__main__":
    print("Testing DefaultFileHasher functionality...")
    test_default_file_hasher_file_hash_consistency()
    test_default_file_hasher_pathset_hash_consistency()
    test_default_file_hasher_packet_hash_consistency()
