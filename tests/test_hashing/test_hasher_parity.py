#!/usr/bin/env python
"""
Test parity between DefaultFileHasher and core hashing functions.

This script directly compares the output of DefaultFileHasher methods against
the corresponding core functions (hash_file, hash_pathset, hash_packet) to ensure
they produce identical results.
"""

import json
import random
from pathlib import Path

import pytest

from orcapod.hashing.legacy_core import hash_file, hash_packet, hash_pathset
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


def test_hasher_core_parity_file_hash():
    """Test that BasicFileHasher.hash_file produces the same results as hash_file."""
    hash_lut = load_hash_lut()
    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite()

    # Test all sample files
    for filename, info in hash_lut.items():
        rel_path = info["file"]
        file_path = verify_path_exists(rel_path)

        # Compare hashes from both implementations
        hasher_result = hasher.hash_file(file_path)
        core_result = hash_file(file_path)

        assert hasher_result == core_result, (
            f"Hash mismatch for {filename}: "
            f"DefaultFileHasher: {hasher_result}, core: {core_result}"
        )
        print(f"Verified hash parity for {filename}")

    # Test with different algorithm parameters
    algorithms = ["sha256", "sha1", "md5", "xxh64", "crc32"]
    buffer_sizes = [1024, 4096, 65536]

    # Pick a random file for testing
    filename, info = random.choice(list(hash_lut.items()))
    file_path = verify_path_exists(info["file"])

    for algorithm in algorithms:
        for buffer_size in buffer_sizes:
            try:
                # Create a hasher with specific parameters
                hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                    algorithm=algorithm, buffer_size=buffer_size
                )

                # Compare hashes
                hasher_result = hasher.hash_file(file_path)
                core_result = hash_file(
                    file_path, algorithm=algorithm, buffer_size=buffer_size
                )

                assert hasher_result == core_result, (
                    f"Hash mismatch for {filename} with algorithm={algorithm}, buffer_size={buffer_size}: "
                    f"DefaultFileHasher: {hasher_result}, core: {core_result}"
                )
                print(
                    f"Verified hash parity for {filename} with algorithm={algorithm}, buffer_size={buffer_size}"
                )
            except ValueError as e:
                print(f"Algorithm {algorithm} not supported: {e}")


def test_hasher_core_parity_pathset_hash():
    """Test that DefaultFileHasher.hash_pathset produces the same results as hash_pathset."""
    hash_lut = load_pathset_hash_lut()

    # Test all sample pathsets
    for name, info in hash_lut.items():
        paths_rel = info["paths"]
        pathset_type = info["type"]

        # Create actual pathset based on type
        if pathset_type == "single_file" or pathset_type == "directory":
            pathset = verify_path_exists(paths_rel[0])
        else:  # Collection
            pathset = [verify_path_exists(p) for p in paths_rel]

        # Compare various configurations
        algorithms = ["sha256", "sha1"]
        buffer_sizes = [4096, 65536]
        char_counts = [16, 32, None]

        for algorithm in algorithms:
            for buffer_size in buffer_sizes:
                for char_count in char_counts:
                    # Create a hasher with specific parameters
                    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                        algorithm=algorithm,
                        buffer_size=buffer_size,
                        char_count=char_count,
                    )

                    # Compare hashes
                    hasher_result = hasher.hash_pathset(pathset)
                    core_result = hash_pathset(
                        pathset,
                        algorithm=algorithm,
                        buffer_size=buffer_size,
                        char_count=char_count,
                    )

                    assert hasher_result == core_result, (
                        f"Hash mismatch for pathset {name} with "
                        f"algorithm={algorithm}, buffer_size={buffer_size}, char_count={char_count}: "
                        f"DefaultFileHasher: {hasher_result}, core: {core_result}"
                    )
                    print(
                        f"Verified pathset hash parity for {name} with "
                        f"algorithm={algorithm}, buffer_size={buffer_size}, char_count={char_count}"
                    )


def test_hasher_core_parity_packet_hash():
    """Test that DefaultFileHasher.hash_packet produces the same results as hash_packet."""
    hash_lut = load_packet_hash_lut()

    # Test with a subset of sample packets to avoid excessive test times
    packet_items = list(hash_lut.items())
    test_items = packet_items[: min(3, len(packet_items))]

    for name, info in test_items:
        structure = info["structure"]

        # Reconstruct the packet
        packet = {}
        for key, value in structure.items():
            if isinstance(value, list):
                packet[key] = [verify_path_exists(p) for p in value]
            else:
                packet[key] = verify_path_exists(value)

        # Compare various configurations
        algorithms = ["sha256", "sha1"]
        buffer_sizes = [4096, 65536]
        char_counts = [16, 32, None]

        for algorithm in algorithms:
            for buffer_size in buffer_sizes:
                for char_count in char_counts:
                    # Create a hasher with specific parameters
                    hasher = LegacyPathLikeHasherFactory.create_basic_legacy_composite(
                        algorithm=algorithm,
                        buffer_size=buffer_size,
                        char_count=char_count,
                    )

                    # Compare hashes
                    hasher_result = hasher.hash_packet(packet)
                    core_result = hash_packet(
                        packet,
                        algorithm=algorithm,
                        buffer_size=buffer_size,
                        char_count=char_count,
                    )

                    assert hasher_result == core_result, (
                        f"Hash mismatch for packet {name} with "
                        f"algorithm={algorithm}, buffer_size={buffer_size}, char_count={char_count}: "
                        f"DefaultFileHasher: {hasher_result}, core: {core_result}"
                    )
                    print(
                        f"Verified packet hash parity for {name} with "
                        f"algorithm={algorithm}, buffer_size={buffer_size}, char_count={char_count}"
                    )


if __name__ == "__main__":
    print("Testing DefaultFileHasher parity with core functions...")
    test_hasher_core_parity_file_hash()
    test_hasher_core_parity_pathset_hash()
    test_hasher_core_parity_packet_hash()
