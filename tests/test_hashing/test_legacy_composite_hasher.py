#!/usr/bin/env python
"""Tests for the CompositeFileHasher implementation."""

from unittest.mock import patch

import pytest

from orcapod.hashing.legacy_core import hash_to_hex
from orcapod.hashing.file_hashers import (
    LegacyDefaultFileHasher,
    LegacyDefaultCompositeFileHasher,
)
from orcapod.hashing.types import (
    LegacyFileHasher,
    LegacyPacketHasher,
    LegacyPathSetHasher,
)


# Custom implementation of hash_file for tests that doesn't check for file existence
def mock_hash_file(file_path, algorithm="sha256", buffer_size=65536) -> str:
    """Mock implementation of hash_file that doesn't check for file existence."""
    # Simply return a deterministic hash based on the file path
    return hash_to_hex(f"mock_file_hash_{file_path}_{algorithm}")


# Custom implementation of hash_pathset for tests that doesn't check for file existence
def mock_hash_pathset(
    pathset, algorithm="sha256", buffer_size=65536, char_count=32, file_hasher=None
):
    """Mock implementation of hash_pathset that doesn't check for file existence."""
    from collections.abc import Collection
    from os import PathLike
    from pathlib import Path

    # If file_hasher is None, we'll need to handle it differently
    if file_hasher is None:
        # Just return a mock hash for testing
        if isinstance(pathset, (str, Path, PathLike)):
            return f"mock_{pathset}"
        return "mock_hash"

    # Handle dictionary case for nested paths
    if isinstance(pathset, dict):
        hash_dict = {}
        for key, value in pathset.items():
            hash_dict[key] = mock_hash_pathset(
                value, algorithm, buffer_size, char_count, file_hasher
            )
        return hash_to_hex(str(hash_dict))

    # Handle collection case (list, set, etc.)
    if isinstance(pathset, Collection) and not isinstance(
        pathset, (str, Path, PathLike)
    ):
        hash_list = []
        for item in pathset:
            hash_list.append(
                mock_hash_pathset(item, algorithm, buffer_size, char_count, file_hasher)
            )
        return hash_to_hex(str(hash_list))

    # Handle simple string or Path case
    if isinstance(pathset, (str, Path, PathLike)):
        if hasattr(file_hasher, "__self__"):  # For bound methods
            return file_hasher(str(pathset))
        else:
            return file_hasher(str(pathset))

    return "mock_hash"


# Custom implementation of hash_packet for tests that doesn't check for file existence
def mock_hash_packet(
    packet,
    algorithm="sha256",
    buffer_size=65536,
    char_count=32,
    prefix_algorithm=True,
    pathset_hasher=None,
):
    """Mock implementation of hash_packet that doesn't check for file existence."""
    # Create a simple hash based on the packet structure
    hash_value = hash_to_hex(str(packet))

    # Format it like the real function would
    if prefix_algorithm and algorithm:
        return (
            f"{algorithm}-{hash_value[: char_count if char_count else len(hash_value)]}"
        )
    else:
        return hash_value[: char_count if char_count else len(hash_value)]


@pytest.fixture(autouse=True)
def patch_hash_functions():
    """Patch the hash functions in the core module for all tests."""
    with (
        patch("orcapod.hashing.legacy_core.hash_file", side_effect=mock_hash_file),
        patch(
            "orcapod.hashing.legacy_core.hash_pathset", side_effect=mock_hash_pathset
        ),
        patch("orcapod.hashing.legacy_core.hash_packet", side_effect=mock_hash_packet),
    ):
        yield


def test_default_composite_hasher_implements_all_protocols():
    """Test that CompositeFileHasher implements all three protocols."""
    # Create a basic file hasher to be used within the composite hasher
    file_hasher = LegacyDefaultFileHasher()

    # Create the composite hasher
    composite_hasher = LegacyDefaultCompositeFileHasher(file_hasher)

    # Verify it implements all three protocols
    assert isinstance(composite_hasher, LegacyFileHasher)
    assert isinstance(composite_hasher, LegacyPathSetHasher)
    assert isinstance(composite_hasher, LegacyPacketHasher)


def test_default_composite_hasher_file_hashing():
    """Test CompositeFileHasher's file hashing functionality."""
    # We can use a mock path since our mocks don't require real files
    file_path = "/path/to/mock_file.txt"

    # Create a custom mock file hasher
    class MockFileHasher:
        def hash_file(self, file_path):
            return mock_hash_file(file_path)

    file_hasher = MockFileHasher()
    composite_hasher = LegacyDefaultCompositeFileHasher(file_hasher)

    # Get hash from the composite hasher and directly from the file hasher
    direct_hash = file_hasher.hash_file(file_path)
    composite_hash = composite_hasher.hash_file(file_path)

    # The hashes should be identical
    assert direct_hash == composite_hash


def test_default_composite_hasher_pathset_hashing():
    """Test CompositeFileHasher's path set hashing functionality."""

    # Create a custom mock file hasher that doesn't check for file existence
    class MockFileHasher:
        def hash_file(self, file_path) -> str:
            return mock_hash_file(file_path)

    file_hasher = MockFileHasher()
    composite_hasher = LegacyDefaultCompositeFileHasher(file_hasher)

    # Simple path set with non-existent paths
    pathset = ["/path/to/file1.txt", "/path/to/file2.txt"]

    # Hash the pathset
    result = composite_hasher.hash_pathset(pathset)

    # The result should be a string hash
    assert isinstance(result, str)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
