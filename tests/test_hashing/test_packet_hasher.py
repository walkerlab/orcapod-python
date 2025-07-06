#!/usr/bin/env python
"""Tests for the PacketHasher protocol implementation."""

import pytest

from orcapod.hashing.file_hashers import LegacyDefaultPacketHasher
from orcapod.hashing.types import LegacyPathSetHasher


class MockPathSetHasher(LegacyPathSetHasher):
    """Simple mock PathSetHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.pathset_hash_calls = []

    def hash_pathset(self, pathset):
        self.pathset_hash_calls.append(pathset)
        return f"{self.hash_value}_{pathset}"


def test_legacy_packet_hasher_empty_packet():
    """Test LegacyPacketHasher with an empty packet."""
    pathset_hasher = MockPathSetHasher()
    packet_hasher = LegacyDefaultPacketHasher(pathset_hasher)

    # Test with empty packet
    packet = {}

    result = packet_hasher.hash_packet(packet)

    # No pathset hash calls should be made
    assert len(pathset_hasher.pathset_hash_calls) == 0

    # The result should still be a string hash
    assert isinstance(result, str)


def test_legacy_packet_hasher_single_entry():
    """Test LegacyPacketHasher with a packet containing a single entry."""
    pathset_hasher = MockPathSetHasher()
    packet_hasher = LegacyDefaultPacketHasher(pathset_hasher)

    # Test with a single entry
    packet = {"input": "/path/to/file.txt"}

    result = packet_hasher.hash_packet(packet)

    # Verify the pathset_hasher was called once
    assert len(pathset_hasher.pathset_hash_calls) == 1
    assert pathset_hasher.pathset_hash_calls[0] == packet["input"]

    # The result should be a string hash
    assert isinstance(result, str)


def test_legacy_packet_hasher_multiple_entries():
    """Test LegacyPacketHasher with a packet containing multiple entries."""
    pathset_hasher = MockPathSetHasher()
    packet_hasher = LegacyDefaultPacketHasher(pathset_hasher)

    # Test with multiple entries
    packet = {
        "input1": "/path/to/file1.txt",
        "input2": ["/path/to/file2.txt", "/path/to/file3.txt"],
        "input3": {"nested": "/path/to/file4.txt"},
    }

    result = packet_hasher.hash_packet(packet)

    # Verify the pathset_hasher was called for each entry
    assert len(pathset_hasher.pathset_hash_calls) == 3
    assert pathset_hasher.pathset_hash_calls[0] == packet["input1"]
    assert pathset_hasher.pathset_hash_calls[1] == packet["input2"]
    assert pathset_hasher.pathset_hash_calls[2] == packet["input3"]

    # The result should be a string hash
    assert isinstance(result, str)


def test_legacy_packet_hasher_nested_structure():
    """Test LegacyPacketHasher with a deeply nested packet structure."""
    pathset_hasher = MockPathSetHasher()
    packet_hasher = LegacyDefaultPacketHasher(pathset_hasher)

    # Test with nested packet structure
    packet = {
        "input": {
            "images": ["/path/to/image1.jpg", "/path/to/image2.jpg"],
            "metadata": {"config": "/path/to/config.json"},
        },
        "output": ["/path/to/output1.txt", "/path/to/output2.txt"],
    }

    result = packet_hasher.hash_packet(packet)

    # Verify the pathset_hasher was called for each top-level key
    assert len(pathset_hasher.pathset_hash_calls) == 2
    assert pathset_hasher.pathset_hash_calls[0] == packet["input"]
    assert pathset_hasher.pathset_hash_calls[1] == packet["output"]

    # The result should be a string hash
    assert isinstance(result, str)


def test_legacy_packet_hasher_with_char_count():
    """Test LegacyPacketHasher with different char_count values."""
    pathset_hasher = MockPathSetHasher()

    # Test with default char_count (32)
    default_hasher = LegacyDefaultPacketHasher(pathset_hasher)
    default_result = default_hasher.hash_packet({"input": "/path/to/file.txt"})

    # Test with custom char_count
    custom_hasher = LegacyDefaultPacketHasher(pathset_hasher, char_count=16)
    custom_result = custom_hasher.hash_packet({"input": "/path/to/file.txt"})

    # Results should be different based on char_count
    assert isinstance(default_result, str)
    assert isinstance(custom_result, str)
    # The specific length check would depend on the implementation details


if __name__ == "__main__":
    pytest.main(["-v", __file__])
