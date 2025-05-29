#!/usr/bin/env python
# filepath: /home/eywalker/workspace/orcabridge/tests/test_hashing/test_composite_hasher.py
"""Tests for the CompositeHasher implementation."""

import pytest

from orcabridge.hashing.file_hashers import CompositeHasher, BasicFileHasher
from orcabridge.hashing.types import FileHasher, PathSetHasher, PacketHasher


def test_composite_hasher_implements_all_protocols():
    """Test that CompositeHasher implements all three protocols."""
    # Create a basic file hasher to be used within the composite hasher
    file_hasher = BasicFileHasher()

    # Create the composite hasher
    composite_hasher = CompositeHasher(file_hasher)

    # Verify it implements all three protocols
    assert isinstance(composite_hasher, FileHasher)
    assert isinstance(composite_hasher, PathSetHasher)
    assert isinstance(composite_hasher, PacketHasher)


def test_composite_hasher_file_hashing():
    """Test CompositeHasher's file hashing functionality."""
    import tempfile
    import os

    # Create a real file for testing
    fd, file_path = tempfile.mkstemp()
    with os.fdopen(fd, "w") as f:
        f.write("Test content for CompositeHasher")

    file_hasher = BasicFileHasher()
    composite_hasher = CompositeHasher(file_hasher)

    # Get hash from the composite hasher and directly from the file hasher
    direct_hash = file_hasher.hash_file(file_path)
    composite_hash = composite_hasher.hash_file(file_path)
    # The hashes should be identical
    assert direct_hash == composite_hash


def test_composite_hasher_pathset_hashing():
    """Test CompositeHasher's path set hashing functionality."""
    file_hasher = BasicFileHasher()
    composite_hasher = CompositeHasher(file_hasher)

    # TODO: the files must be real file or at least mocked in order for
    # pathset hashing to workcorrectly. Alternatively should use mock FileHasher
    # Simple path set
    pathset = ["/path/to/file1.txt", "/path/to/file2.txt"]

    # Hash the pathset
    result = composite_hasher.hash_pathset(pathset)

    # The result should be a string hash
    assert isinstance(result, str)


def test_composite_hasher_packet_hashing():
    """Test CompositeHasher's packet hashing functionality."""
    file_hasher = BasicFileHasher()
    composite_hasher = CompositeHasher(file_hasher)

    # Simple packet
    packet = {
        "input": ["/path/to/input1.txt", "/path/to/input2.txt"],
        "output": "/path/to/output.txt",
    }

    # Hash the packet
    result = composite_hasher.hash_packet(packet)

    # The result should be a string hash
    assert isinstance(result, str)


def test_composite_hasher_with_char_count():
    """Test CompositeHasher with different char_count values."""
    file_hasher = BasicFileHasher()

    # Test with default char_count
    default_composite = CompositeHasher(file_hasher)

    # Test with custom char_count
    custom_composite = CompositeHasher(file_hasher, char_count=16)

    # Simple test data
    pathset = ["/path/to/file1.txt", "/path/to/file2.txt"]
    packet = {"input": pathset}

    # Get hashes with different char_counts
    default_pathset_hash = default_composite.hash_pathset(pathset)
    custom_pathset_hash = custom_composite.hash_pathset(pathset)

    default_packet_hash = default_composite.hash_packet(packet)
    custom_packet_hash = custom_composite.hash_packet(packet)

    # Verify all results are strings
    assert isinstance(default_pathset_hash, str)
    assert isinstance(custom_pathset_hash, str)
    assert isinstance(default_packet_hash, str)
    assert isinstance(custom_packet_hash, str)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
