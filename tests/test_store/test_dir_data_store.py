#!/usr/bin/env python
"""Tests for DirDataStore."""

import json
import shutil
from pathlib import Path

import pytest

from orcapod.hashing.types import (
    LegacyCompositeFileHasher,
    LegacyFileHasher,
    LegacyPacketHasher,
    LegacyPathSetHasher,
)
from orcapod.databases.legacy.dict_data_stores import DirDataStore


class MockFileHasher(LegacyFileHasher):
    """Mock FileHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.file_hash_calls = []

    def hash_file(self, file_path):
        self.file_hash_calls.append(file_path)
        return f"{self.hash_value}_file"


class MockPathSetHasher(LegacyPathSetHasher):
    """Mock PathSetHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.pathset_hash_calls = []

    def hash_pathset(self, pathset) -> str:
        self.pathset_hash_calls.append(pathset)
        return f"{self.hash_value}_pathset"


class MockPacketHasher(LegacyPacketHasher):
    """Mock PacketHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.packet_hash_calls = []

    def hash_packet(self, packet):
        self.packet_hash_calls.append(packet)
        return f"{self.hash_value}_packet"


class MockCompositeHasher(LegacyCompositeFileHasher):
    """Mock CompositeHasher that implements all three hash protocols."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.file_hash_calls = []
        self.pathset_hash_calls = []
        self.packet_hash_calls = []

    def hash_file_content(self, file_path):
        self.file_hash_calls.append(file_path)
        return f"{self.hash_value}_file"

    def hash_pathset(self, pathset) -> str:
        self.pathset_hash_calls.append(pathset)
        return f"{self.hash_value}_pathset"

    def hash_packet(self, packet) -> str:
        self.packet_hash_calls.append(packet)
        return f"{self.hash_value}_packet"


def test_dir_data_store_init_default_hasher(temp_dir):
    """Test DirDataStore initialization with default PacketHasher."""
    store_dir = Path(temp_dir) / "test_store"

    # Create store with default hasher
    store = DirDataStore(store_dir=store_dir)

    # Check that the store directory was created
    assert store_dir.exists()
    assert store_dir.is_dir()

    # Verify the default PacketHasher is used
    assert isinstance(store.packet_hasher, LegacyPacketHasher)

    # Check default parameters
    assert store.copy_files is True
    assert store.preserve_filename is True
    assert store.overwrite is False
    assert store.supplement_source is False
    assert store.store_dir == store_dir


def test_dir_data_store_init_custom_hasher(temp_dir):
    """Test DirDataStore initialization with custom PacketHasher."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher()

    # Create store with custom hasher and parameters
    store = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=False,
        preserve_filename=False,
        overwrite=True,
        supplement_source=True,
    )

    # Check that the store directory was created
    assert store_dir.exists()
    assert store_dir.is_dir()

    # Verify our custom PacketHasher is used
    assert store.packet_hasher is packet_hasher

    # Check custom parameters
    assert store.copy_files is False
    assert store.preserve_filename is False
    assert store.overwrite is True
    assert store.supplement_source is True
    assert store.store_dir == store_dir


def test_dir_data_store_memoize_with_file_copy(temp_dir, sample_files):
    """Test DirDataStore memoize with file copying enabled."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    store = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=True,
        preserve_filename=True,
    )

    # Create simple packet and output packet
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Memoize the packet and output
    result = store.memoize(
        "test_memoization", "content_hash_123", packet, output_packet
    )

    # The path to where everything should be stored
    expected_store_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )

    # Check that files were created
    assert (expected_store_path / "_info.json").exists()
    assert (expected_store_path / "_source.json").exists()
    assert (expected_store_path / "output1.txt").exists()  # Preserved filename

    # Check the content of the source file
    with open(expected_store_path / "_source.json", "r") as f:
        saved_source = json.load(f)
        assert saved_source == packet

    # Check the content of the info file
    with open(expected_store_path / "_info.json", "r") as f:
        saved_info = json.load(f)
        assert "output_file" in saved_info
        assert saved_info["output_file"] == "output1.txt"  # Relative path

    # Check that the result has the absolute path
    assert result["output_file"] == str(expected_store_path / "output1.txt")


def test_dir_data_store_memoize_without_file_copy(temp_dir, sample_files):
    """Test DirDataStore memoize without file copying."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    store = DirDataStore(
        store_dir=store_dir, packet_hasher=packet_hasher, copy_files=False
    )

    # Create simple packet and output packet
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Memoize the packet and output
    result = store.memoize(  # noqa: F841
        "test_memoization", "content_hash_123", packet, output_packet
    )

    # The path to where everything should be stored
    expected_store_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )

    # Check that info files were created
    assert (expected_store_path / "_info.json").exists()
    assert (expected_store_path / "_source.json").exists()

    # Check that the output file was NOT copied
    assert not (expected_store_path / "output1.txt").exists()

    # Check the content of the source file
    with open(expected_store_path / "_source.json", "r") as f:
        saved_source = json.load(f)
        assert saved_source == packet

    # Check the content of the info file
    with open(expected_store_path / "_info.json", "r") as f:
        saved_info = json.load(f)
        assert saved_info == output_packet  # Original paths preserved


def test_dir_data_store_memoize_without_filename_preservation(temp_dir, sample_files):
    """Test DirDataStore memoize without filename preservation."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    store = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=True,
        preserve_filename=False,
    )

    # Create simple packet and output packet
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Memoize the packet and output
    result = store.memoize(  # noqa: F841
        "test_memoization", "content_hash_123", packet, output_packet
    )

    # The path to where everything should be stored
    expected_store_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )

    # Check that files were created
    assert (expected_store_path / "_info.json").exists()
    assert (expected_store_path / "_source.json").exists()
    assert (
        expected_store_path / "output_file.txt"
    ).exists()  # Key name used, with original extension

    # Check that the output file has expected content
    with open(expected_store_path / "output_file.txt", "r") as f:
        content = f.read()
        assert content == "Output content 1"


def test_dir_data_store_retrieve_memoized(temp_dir, sample_files):
    """Test DirDataStore retrieve_memoized functionality."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    store = DirDataStore(
        store_dir=store_dir, packet_hasher=packet_hasher, copy_files=True
    )

    # Create and memoize a packet
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    store.memoize("test_memoization", "content_hash_123", packet, output_packet)

    # Now retrieve the memoized packet
    retrieved = store.retrieve_memoized("test_memoization", "content_hash_123", packet)

    # The path to where everything should be stored
    expected_store_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )

    # Check that we got a result
    assert retrieved is not None
    assert "output_file" in retrieved
    assert retrieved["output_file"] == str(expected_store_path / "output1.txt")


def test_dir_data_store_retrieve_memoized_nonexistent(temp_dir):
    """Test DirDataStore retrieve_memoized with non-existent data."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    store = DirDataStore(store_dir=store_dir, packet_hasher=packet_hasher)

    # Try to retrieve a non-existent packet
    packet = {"input_file": "nonexistent.txt"}
    retrieved = store.retrieve_memoized("test_memoization", "content_hash_123", packet)

    # Should return None for non-existent data
    assert retrieved is None


def test_dir_data_store_retrieve_memoized_with_supplement(temp_dir, sample_files):
    """Test DirDataStore retrieve_memoized with source supplementation."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    # Create store without source supplementation
    store_without_supplement = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=True,
        supplement_source=False,
    )

    # Create the directory structure and info file, but no source file
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}  # noqa: F841

    storage_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )
    storage_path.mkdir(parents=True, exist_ok=True)

    # Create just the info file (no source file)
    with open(storage_path / "_info.json", "w") as f:
        json.dump({"output_file": "output1.txt"}, f)

    # Copy the output file
    shutil.copy(sample_files["output"]["output1"], storage_path / "output1.txt")

    # Retrieve without supplement - should not create source file
    store_without_supplement.retrieve_memoized(
        "test_memoization", "content_hash_123", packet
    )
    assert not (storage_path / "_source.json").exists()

    # Now with supplement enabled
    store_with_supplement = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=True,
        supplement_source=True,
    )

    # Retrieve with supplement - should create source file
    store_with_supplement.retrieve_memoized(
        "test_memoization", "content_hash_123", packet
    )
    assert (storage_path / "_source.json").exists()

    # Check that the source file has expected content
    with open(storage_path / "_source.json", "r") as f:
        saved_source = json.load(f)
        assert saved_source == packet


def test_dir_data_store_memoize_with_overwrite(temp_dir, sample_files):
    """Test DirDataStore memoize with overwrite enabled."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher(hash_value="fixed_hash")

    # Create store with overwrite disabled (default)
    store_no_overwrite = DirDataStore(
        store_dir=store_dir, packet_hasher=packet_hasher, copy_files=True
    )

    # Create initial packet and output
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet1 = {"output_file": sample_files["output"]["output1"]}

    # First memoization should work fine
    store_no_overwrite.memoize(
        "test_memoization", "content_hash_123", packet, output_packet1
    )

    # Second memoization should raise an error
    output_packet2 = {"output_file": sample_files["output"]["output2"]}
    with pytest.raises(ValueError):
        store_no_overwrite.memoize(
            "test_memoization", "content_hash_123", packet, output_packet2
        )

    # Create store with overwrite enabled
    store_with_overwrite = DirDataStore(
        store_dir=store_dir,
        packet_hasher=packet_hasher,
        copy_files=True,
        overwrite=True,
    )

    # This should work now with overwrite
    result = store_with_overwrite.memoize(
        "test_memoization", "content_hash_123", packet, output_packet2
    )

    # Check that we got the updated output
    expected_store_path = (
        store_dir / "test_memoization" / "content_hash_123" / "fixed_hash_packet"
    )
    assert result["output_file"] == str(expected_store_path / "output2.txt")

    # Check the file was actually overwritten
    with open(expected_store_path / "output2.txt", "r") as f:
        content = f.read()
        assert content == "Output content 2"


def test_dir_data_store_clear_store(temp_dir, sample_files):
    """Test DirDataStore clear_store functionality."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher()

    store = DirDataStore(store_dir=store_dir, packet_hasher=packet_hasher)

    # Create and memoize packets in different stores
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    store.memoize("store1", "content_hash_123", packet, output_packet)
    store.memoize("store2", "content_hash_123", packet, output_packet)

    # Verify both stores exist
    assert (store_dir / "store1").exists()
    assert (store_dir / "store2").exists()

    # Clear store1
    store.clear_store("store1")

    # Check that store1 was deleted but store2 remains
    assert not (store_dir / "store1").exists()
    assert (store_dir / "store2").exists()


def test_dir_data_store_clear_all_stores(temp_dir, sample_files):
    """Test DirDataStore clear_all_stores functionality with force."""
    store_dir = Path(temp_dir) / "test_store"
    packet_hasher = MockPacketHasher()

    store = DirDataStore(store_dir=store_dir, packet_hasher=packet_hasher)

    # Create and memoize packets in different stores
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    store.memoize("store1", "content_hash_123", packet, output_packet)
    store.memoize("store2", "content_hash_123", packet, output_packet)

    # Verify both stores exist
    assert (store_dir / "store1").exists()
    assert (store_dir / "store2").exists()

    # Clear all stores with force and non-interactive mode
    store.clear_all_stores(interactive=False, function_name=str(store_dir), force=True)

    # Check that the entire store directory was deleted
    assert not store_dir.exists()


def test_dir_data_store_with_default_packet_hasher(temp_dir, sample_files):
    """Test DirDataStore using the default CompositeHasher."""
    store_dir = Path(temp_dir) / "test_store"

    # Create store with default FileHasher
    store = DirDataStore(store_dir=store_dir)

    # Verify that default PacketHasher was created
    assert isinstance(store.packet_hasher, LegacyPacketHasher)

    # Test memoization and retrieval
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    result = store.memoize(
        "default_hasher_test", "content_hash_123", packet, output_packet
    )

    # The retrieved packet should have absolute paths
    path = result["output_file"]
    assert str(path).startswith(str(store_dir))


def test_dir_data_store_legacy_mode_compatibility(temp_dir, sample_files):
    """Test that DirDataStore legacy_mode produces identical results to default FileHasher."""
    # Create two store directories
    store_dir_legacy = Path(temp_dir) / "test_store_legacy"
    store_dir_default = Path(temp_dir) / "test_store_default"

    # Create two stores: one with legacy_mode=True, one with the default PacketHasher
    store_legacy = DirDataStore(
        store_dir=store_dir_legacy,
        legacy_mode=True,
        legacy_algorithm="sha256",  # This is the default algorithm
    )

    store_default = DirDataStore(
        store_dir=store_dir_default,
        legacy_mode=False,  # default
    )

    # Test data
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Get the hash values directly for comparison
    from orcapod.hashing.legacy_core import hash_packet

    legacy_hash = hash_packet(packet, algorithm="sha256")
    assert store_default.packet_hasher is not None, (
        "Default store should have a packet hasher"
    )
    default_hash = store_default.packet_hasher.hash_packet(packet)

    # The hashes should be identical since both implementations should produce the same result
    assert legacy_hash == default_hash

    # But both stores should handle the memoization correctly
    result_legacy = store_legacy.memoize(
        "test_compatibility", "content_hash_123", packet, output_packet
    )

    result_default = store_default.memoize(
        "test_compatibility", "content_hash_123", packet, output_packet
    )

    # Both should store and retrieve the output correctly
    assert "output_file" in result_legacy
    assert "output_file" in result_default

    # Check that both stores can retrieve their own memoized data
    retrieved_legacy = store_legacy.retrieve_memoized(
        "test_compatibility", "content_hash_123", packet
    )

    retrieved_default = store_default.retrieve_memoized(
        "test_compatibility", "content_hash_123", packet
    )

    # Both retrievals should succeed
    assert retrieved_legacy is not None
    assert (
        retrieved_default is not None
    )  # Content should be the same, even if paths differ
    assert (
        Path(str(retrieved_legacy["output_file"])).name
        == Path(str(retrieved_default["output_file"])).name
    )

    # Since the hashes are identical, verify that default store CAN find the legacy store's data and vice versa
    # This confirms they use compatible hash computation methods

    # Create a new store instance pointing to the other store's directory
    cross_store_default = DirDataStore(
        store_dir=store_dir_legacy,
        legacy_mode=False,  # default
    )

    cross_retrieve_default = cross_store_default.retrieve_memoized(
        "test_compatibility", "content_hash_123", packet
    )

    # Since the hash computation is identical, the default store should find the legacy store's data
    assert cross_retrieve_default is not None
    assert "output_file" in cross_retrieve_default


def test_dir_data_store_legacy_mode_fallback(temp_dir, sample_files):
    """Test that we can use legacy_mode to access data stored with the old hashing method."""
    # Create a store directory
    store_dir = Path(temp_dir) / "test_store"

    # First, store data using legacy mode
    legacy_store = DirDataStore(store_dir=store_dir, legacy_mode=True)

    # Test data
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # Store data using legacy mode
    legacy_store.memoize("test_fallback", "content_hash_123", packet, output_packet)

    # Now create a new store with legacy_mode=True to retrieve the data
    fallback_store = DirDataStore(store_dir=store_dir, legacy_mode=True)

    # Try to retrieve the data
    retrieved = fallback_store.retrieve_memoized(
        "test_fallback", "content_hash_123", packet
    )

    # Should successfully retrieve the data
    assert retrieved is not None
    assert "output_file" in retrieved

    # Now try with a default store (legacy_mode=False)
    default_store = DirDataStore(store_dir=store_dir, legacy_mode=False)

    # Try to retrieve the data
    retrieved_default = default_store.retrieve_memoized(
        "test_fallback", "content_hash_123", packet
    )

    # Should find the data, since the hash computation is identical
    assert retrieved_default is not None
    assert "output_file" in retrieved_default


def test_dir_data_store_hash_equivalence(temp_dir, sample_files):
    """Test that hash_packet and packet_hasher.hash_packet produce identical directory structures."""
    # Create a store directory
    store_dir = Path(temp_dir) / "test_store"

    # Create test data
    packet = {"input_file": sample_files["input"]["file1"]}
    output_packet = {"output_file": sample_files["output"]["output1"]}

    # First compute hashes directly
    from orcapod.hashing.legacy_core import hash_packet
    from orcapod.hashing.defaults import get_default_composite_file_hasher

    legacy_hash = hash_packet(packet, algorithm="sha256")
    default_hasher = get_default_composite_file_hasher(
        with_cache=False
    )  # No caching for direct comparison
    default_hash = default_hasher.hash_packet(packet)

    # Verify that the hash values are identical
    assert legacy_hash == default_hash, (
        "Legacy hash and default hash should be identical"
    )

    # Create stores with both methods
    legacy_store = DirDataStore(
        store_dir=store_dir, legacy_mode=True, legacy_algorithm="sha256"
    )

    default_store = DirDataStore(
        store_dir=store_dir, legacy_mode=False, packet_hasher=default_hasher
    )

    # Store data using legacy mode
    legacy_result = legacy_store.memoize(
        "test_equivalence", "content_hash_123", packet, output_packet
    )

    # Verify directory structure
    expected_path = (
        store_dir / "test_equivalence" / "content_hash_123" / str(legacy_hash)
    )
    assert expected_path.exists(), "Legacy hash directory should exist"

    # Retrieve using default store (without using memoize, just retrieve)
    default_result = default_store.retrieve_memoized(
        "test_equivalence", "content_hash_123", packet
    )

    # Should be able to retrieve data stored using legacy mode
    assert default_result is not None
    assert "output_file" in default_result

    # The retrieved paths should point to the same files (even if possibly formatted differently)
    legacy_file = Path(str(legacy_result["output_file"]))
    default_file = Path(str(default_result["output_file"]))

    assert legacy_file.exists()
    assert default_file.exists()
    assert legacy_file.samefile(default_file), (
        "Both modes should access the same physical files"
    )


if __name__ == "__main__":
    pytest.main(["-v", __file__])
