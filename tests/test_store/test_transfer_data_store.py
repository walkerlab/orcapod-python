#!/usr/bin/env python
"""Tests for TransferDataStore."""

from pathlib import Path

import pytest

from orcapod.hashing.types import LegacyPacketHasher
from orcapod.databases.legacy.dict_data_stores import DirDataStore, NoOpDataStore
from orcapod.databases.legacy.dict_transfer_data_store import TransferDataStore


class MockPacketHasher(LegacyPacketHasher):
    """Mock PacketHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.packet_hash_calls = []

    def hash_packet(self, packet):
        self.packet_hash_calls.append(packet)
        return f"{self.hash_value}_packet"


def test_transfer_data_store_basic_setup(temp_dir, sample_files):
    """Test basic setup of TransferDataStore."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)

    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Verify the stores are set correctly
    assert transfer_store.source_store is source_store
    assert transfer_store.target_store is target_store


def test_transfer_data_store_memoize_to_target(temp_dir, sample_files):
    """Test that memoize stores packets in the target store."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Memoize through transfer store
    result = transfer_store.memoize(
        "test_store", "content_hash_123", packet, output_packet
    )

    # Verify the packet was stored in target store
    assert "output_file" in result

    # Verify we can retrieve it directly from target store
    retrieved_from_target = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target is not None
    assert "output_file" in retrieved_from_target

    # Verify it's NOT in the source store
    retrieved_from_source = source_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_source is None


def test_transfer_data_store_retrieve_from_target_first(temp_dir, sample_files):
    """Test that retrieve_memoized checks target store first."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Store directly in target store
    target_store.memoize("test_store", "content_hash_123", packet, output_packet)

    # Retrieve through transfer store should find it in target
    result = transfer_store.retrieve_memoized("test_store", "content_hash_123", packet)

    assert result is not None
    assert "output_file" in result


def test_transfer_data_store_fallback_to_source_and_copy(temp_dir, sample_files):
    """Test that retrieve_memoized falls back to source store and copies to target."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Store only in source store
    source_store.memoize("test_store", "content_hash_123", packet, output_packet)

    # Verify it's not in target initially
    retrieved_from_target = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target is None

    # Retrieve through transfer store should find it in source and copy to target
    result = transfer_store.retrieve_memoized("test_store", "content_hash_123", packet)

    assert result is not None
    assert "output_file" in result

    # Now verify it was copied to target store
    retrieved_from_target_after = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target_after is not None
    assert "output_file" in retrieved_from_target_after


def test_transfer_data_store_multiple_packets(temp_dir, sample_files):
    """Test transfer functionality with multiple packets."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create multiple packets
    packets = [
        {"input_file": sample_files["input"]["file1"]},
        {"input_file": sample_files["input"]["file2"]},
    ]

    output_packets = [
        {"output_file": sample_files["output"]["output1"]},
        {"output_file": sample_files["output"]["output2"]},
    ]

    content_hashes = ["content_hash_1", "content_hash_2"]

    # Store all packets in source store
    for i, (packet, output_packet, content_hash) in enumerate(
        zip(packets, output_packets, content_hashes)
    ):
        source_store.memoize("test_store", content_hash, packet, output_packet)

    # Verify none are in target initially
    for packet, content_hash in zip(packets, content_hashes):
        retrieved = target_store.retrieve_memoized("test_store", content_hash, packet)
        assert retrieved is None

    # Retrieve all packets through transfer store
    results = []
    for packet, content_hash in zip(packets, content_hashes):
        result = transfer_store.retrieve_memoized("test_store", content_hash, packet)
        assert result is not None
        results.append(result)

    # Verify all packets are now in target store
    for packet, content_hash in zip(packets, content_hashes):
        retrieved = target_store.retrieve_memoized("test_store", content_hash, packet)
        assert retrieved is not None
        assert "output_file" in retrieved


def test_transfer_data_store_explicit_transfer_method(temp_dir, sample_files):
    """Test the explicit transfer method."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Store in source store
    source_store.memoize("test_store", "content_hash_123", packet, output_packet)

    # Use explicit transfer method
    result = transfer_store.transfer("test_store", "content_hash_123", packet)

    assert result is not None
    assert "output_file" in result

    # Verify it's now in target store
    retrieved_from_target = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target is not None


def test_transfer_data_store_transfer_method_not_found(temp_dir, sample_files):
    """Test transfer method raises error when packet not found in source."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet
    packet = {"input_file": sample_files["input"]["file1"]}

    # Try to transfer packet that doesn't exist
    with pytest.raises(ValueError, match="Packet not found in source store"):
        transfer_store.transfer("test_store", "nonexistent_hash", packet)


def test_transfer_data_store_retrieve_nonexistent_packet(temp_dir, sample_files):
    """Test retrieve_memoized returns None for nonexistent packets."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    source_store = DirDataStore(store_dir=source_store_dir)
    target_store = DirDataStore(store_dir=target_store_dir)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet
    packet = {"input_file": sample_files["input"]["file1"]}

    # Try to retrieve nonexistent packet
    result = transfer_store.retrieve_memoized("test_store", "nonexistent_hash", packet)
    assert result is None


def test_transfer_data_store_different_file_hashers(temp_dir, sample_files):
    """Test transfer between stores with different file hashers."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    # Create stores with different hashers
    source_hasher = MockPacketHasher(hash_value="source_hash")
    target_hasher = MockPacketHasher(hash_value="target_hash")

    source_store = DirDataStore(store_dir=source_store_dir, packet_hasher=source_hasher)
    target_store = DirDataStore(store_dir=target_store_dir, packet_hasher=target_hasher)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Store in source store
    source_store.memoize("test_store", "content_hash_123", packet, output_packet)

    # Verify it's in source store using source hasher
    retrieved_from_source = source_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_source is not None

    # Transfer through transfer store - this should work despite different hashers
    result = transfer_store.retrieve_memoized("test_store", "content_hash_123", packet)
    assert result is not None
    assert "output_file" in result

    # Verify it's now in target store using target hasher
    retrieved_from_target = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target is not None

    # Verify both hashers were called
    assert len(source_hasher.packet_hash_calls) > 0
    assert len(target_hasher.packet_hash_calls) > 0


def test_transfer_data_store_memoize_new_packet_with_different_hashers(
    temp_dir, sample_files
):
    """Test memoizing new packets when source and target have different hashers."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    # Create stores with different hashers
    source_hasher = MockPacketHasher(hash_value="source_hash")
    target_hasher = MockPacketHasher(hash_value="target_hash")

    source_store = DirDataStore(store_dir=source_store_dir, packet_hasher=source_hasher)
    target_store = DirDataStore(store_dir=target_store_dir, packet_hasher=target_hasher)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Memoize through transfer store (should go to target)
    result = transfer_store.memoize(
        "test_store", "content_hash_123", packet, output_packet
    )

    assert result is not None
    assert "output_file" in result

    # Verify it's only in target store, not source
    retrieved_from_target = target_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_target is not None

    retrieved_from_source = source_store.retrieve_memoized(
        "test_store", "content_hash_123", packet
    )
    assert retrieved_from_source is None

    # Verify target hasher was used for memoization
    assert len(target_hasher.packet_hash_calls) > 0


def test_transfer_data_store_complex_transfer_scenario(temp_dir, sample_files):
    """Test complex scenario with multiple operations and different hashers."""
    source_store_dir = Path(temp_dir) / "source_store"
    target_store_dir = Path(temp_dir) / "target_store"

    # Create stores with different hashers
    source_hasher = MockPacketHasher(hash_value="source_hash")
    target_hasher = MockPacketHasher(hash_value="target_hash")

    source_store = DirDataStore(store_dir=source_store_dir, packet_hasher=source_hasher)
    target_store = DirDataStore(store_dir=target_store_dir, packet_hasher=target_hasher)
    transfer_store = TransferDataStore(
        source_store=source_store, target_store=target_store
    )

    # Create multiple packets
    packets = [
        {"input_file": sample_files["input"]["file1"]},
        {"input_file": sample_files["input"]["file2"]},
    ]

    output_packets = [
        {"output_file": sample_files["output"]["output1"]},
        {"output_file": sample_files["output"]["output2"]},
    ]

    content_hashes = ["content_hash_1", "content_hash_2"]

    # 1. Store first packet directly in source
    source_store.memoize("test_store", content_hashes[0], packets[0], output_packets[0])

    # 2. Store second packet through transfer store (should go to target)
    transfer_store.memoize(
        "test_store", content_hashes[1], packets[1], output_packets[1]
    )

    # 3. Retrieve first packet through transfer store (should copy from source to target)
    result1 = transfer_store.retrieve_memoized(
        "test_store", content_hashes[0], packets[0]
    )
    assert result1 is not None

    # 4. Retrieve second packet through transfer store (should find in target directly)
    result2 = transfer_store.retrieve_memoized(
        "test_store", content_hashes[1], packets[1]
    )
    assert result2 is not None

    # 5. Verify both packets are now in target store
    for packet, content_hash in zip(packets, content_hashes):
        retrieved = target_store.retrieve_memoized("test_store", content_hash, packet)
        assert retrieved is not None
        assert "output_file" in retrieved

    # 6. Verify first packet is still in source, second is not
    retrieved_source_1 = source_store.retrieve_memoized(
        "test_store", content_hashes[0], packets[0]
    )
    assert retrieved_source_1 is not None

    retrieved_source_2 = source_store.retrieve_memoized(
        "test_store", content_hashes[1], packets[1]
    )
    assert retrieved_source_2 is None


def test_transfer_data_store_with_noop_stores(temp_dir, sample_files):
    """Test transfer store behavior with NoOpDataStore."""
    # Test with NoOp as source
    noop_source = NoOpDataStore()
    target_store_dir = Path(temp_dir) / "target_store"
    target_store = DirDataStore(store_dir=target_store_dir)

    transfer_store = TransferDataStore(
        source_store=noop_source, target_store=target_store
    )

    packet = {"input": sample_files["input"]["file1"]}

    # Should return None since NoOp store doesn't store anything
    result = transfer_store.retrieve_memoized("test_store", "hash123", packet)
    assert result is None

    # Test with NoOp as target
    source_store_dir = Path(temp_dir) / "source_store"
    source_store = DirDataStore(store_dir=source_store_dir)
    noop_target = NoOpDataStore()

    transfer_store2 = TransferDataStore(
        source_store=source_store, target_store=noop_target
    )

    output_packet = {"output": sample_files["output"]["output1"]}

    # Memoize should work (goes to target which is NoOp)
    result = transfer_store2.memoize("test_store", "hash123", packet, output_packet)
    assert result == output_packet  # NoOp just returns the output packet


if __name__ == "__main__":
    pytest.main(["-v", __file__])
