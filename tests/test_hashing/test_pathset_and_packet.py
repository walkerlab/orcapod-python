#!/usr/bin/env python
"""
Test the hash_pathset and hash_packet functions from orcapod.hashing.

This module contains tests to verify the correct behavior of hash_pathset and hash_packet
functions with various input types and configurations.
"""

import logging
import os
import tempfile
from pathlib import Path

import pytest

from orcapod.hashing.legacy_core import hash_file, hash_packet, hash_pathset

logger = logging.getLogger(__name__)


def test_hash_pathset_single_file():
    """Test hashing of a single file path."""
    # Create a temporary file with known content
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"Test content for hash_pathset")
        temp_path = temp_file.name

    try:
        # Hash the file using different methods
        hash1 = hash_pathset(temp_path)
        hash2 = hash_pathset(Path(temp_path))
        hash3 = hash_file(temp_path)

        # All hashes should match
        assert hash1 == hash2, (
            "Hash should be the same regardless of path type (str or Path)"
        )
        assert hash1 == hash3, "For a single file, hash_pathset should equal hash_file"

        # Test with different algorithms
        sha256_hash = hash_pathset(temp_path, algorithm="sha256")
        sha1_hash = hash_pathset(temp_path, algorithm="sha1")
        md5_hash = hash_pathset(temp_path, algorithm="md5")

        # Different algorithms should produce different hashes
        assert sha256_hash != sha1_hash, (
            "Different algorithms should produce different hashes"
        )
        assert sha1_hash != md5_hash, (
            "Different algorithms should produce different hashes"
        )
        assert md5_hash != sha256_hash, (
            "Different algorithms should produce different hashes"
        )

        # Test with different character counts
        short_buffer = hash_pathset(temp_path, buffer_size=1024)
        long_buffer = hash_pathset(temp_path, buffer_size=6096)

        assert short_buffer == long_buffer, (
            "Buffer size should not affect resulting hashes"
        )

    finally:
        # Clean up
        os.unlink(temp_path)


def test_hash_pathset_directory():
    """Test hashing of a directory containing multiple files."""
    # Create a temporary directory with multiple files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a few files with different content
        file1_path = os.path.join(temp_dir, "file1.txt")
        file2_path = os.path.join(temp_dir, "file2.txt")
        subdir_path = os.path.join(temp_dir, "subdir")
        os.mkdir(subdir_path)
        file3_path = os.path.join(subdir_path, "file3.txt")

        with open(file1_path, "w") as f:
            f.write("Content of file 1")
        with open(file2_path, "w") as f:
            f.write("Content of file 2")
        with open(file3_path, "w") as f:
            f.write("Content of file 3")

        # Hash the directory
        dir_hash = hash_pathset(temp_dir)

        # Hash should be consistent
        assert hash_pathset(temp_dir) == dir_hash, "Directory hash should be consistent"

        # Test that changing content changes the hash
        with open(file1_path, "w") as f:
            f.write("Modified content of file 1")

        modified_dir_hash = hash_pathset(temp_dir)
        assert modified_dir_hash != dir_hash, (
            "Hash should change when file content changes"
        )

        # Test that adding a file changes the hash
        file4_path = os.path.join(temp_dir, "file4.txt")
        with open(file4_path, "w") as f:
            f.write("Content of file 4")

        added_file_hash = hash_pathset(temp_dir)
        assert added_file_hash != modified_dir_hash, (
            "Hash should change when adding files"
        )


def test_hash_pathset_collection():
    """Test hashing of a collection of file paths."""
    # Create temporary files
    temp_files = []
    try:
        for i in range(3):
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(f"Content of file {i}".encode())
                temp_files.append(temp_file.name)

        # Hash the collection
        collection_hash = hash_pathset(temp_files)

        # Hash should be consistent
        assert hash_pathset(temp_files) == collection_hash, (
            "Collection hash should be consistent"
        )

        # Order of files shouldn't matter because we use path names as keys
        reversed_files = list(reversed(temp_files))
        reversed_hash = hash_pathset(reversed_files)
        assert reversed_hash == collection_hash, (
            "Order of files shouldn't affect the hash"
        )

        # Test with Path objects
        path_objects = [Path(f) for f in temp_files]
        path_hash = hash_pathset(path_objects)
        assert path_hash == collection_hash, (
            "Path objects should hash the same as strings"
        )

        # Test that changing content changes the hash
        with open(temp_files[0], "w") as f:
            f.write("Modified content")

        modified_collection_hash = hash_pathset(temp_files)
        assert modified_collection_hash != collection_hash, (
            "Hash should change when content changes"
        )

    finally:
        # Clean up
        for file_path in temp_files:
            try:
                os.unlink(file_path)
            except Exception as e:
                logger.error(f"Error cleaning up file {file_path}: {e}")
                pass


def test_hash_pathset_edge_cases():
    """Test hash_pathset with edge cases."""
    # Test with a non-existent file
    with pytest.raises(FileNotFoundError):
        hash_pathset("/path/to/nonexistent/file")

    # Test with an empty collection
    assert hash_pathset([]) == hash_pathset(()), (
        "Empty collections should hash the same"
    )

    # Test with a collection containing None (should raise an error)
    with pytest.raises(NotImplementedError):
        hash_pathset([None])


def test_hash_packet_basic():
    """Test basic functionality of hash_packet."""
    # Create temporary files for testing
    temp_files = []
    try:
        for i in range(3):
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(f"Content for packet test file {i}".encode())
                temp_files.append(temp_file.name)

        # Create a packet (dictionary mapping keys to files or collections of files)
        packet = {
            "key1": temp_files[0],
            "key2": [temp_files[1], temp_files[2]],
        }

        # Test basic hashing
        packet_hash = hash_packet(packet)

        # Hash should be consistent
        assert hash_packet(packet) == packet_hash, "Packet hash should be consistent"

        # Hash should start with algorithm name by default
        assert packet_hash.startswith("sha256-"), (
            "Packet hash should be prefixed with algorithm"
        )

        # Test without algorithm prefix
        no_prefix_hash = hash_packet(packet, prefix_algorithm=False)
        assert not no_prefix_hash.startswith("sha256-"), (
            "Hash should not have algorithm prefix"
        )

        # Test with different algorithm
        md5_hash = hash_packet(packet, algorithm="md5")
        assert md5_hash.startswith("md5-"), (
            "Hash should be prefixed with specified algorithm"
        )
        assert md5_hash != packet_hash, (
            "Different algorithms should produce different hashes"
        )

        # Test with different char_count
        short_hash = hash_packet(packet, char_count=16, prefix_algorithm=False)
        assert len(short_hash) == 16, "Should respect char_count parameter"

    finally:
        # Clean up
        for file_path in temp_files:
            try:
                os.unlink(file_path)
            except Exception as e:
                logger.error(f"Error cleaning up file {file_path}: {e}")
                pass


def test_hash_packet_content_changes():
    """Test that hash_packet changes when content changes."""
    # Create temp directory with files
    with tempfile.TemporaryDirectory() as temp_dir:
        file1_path = os.path.join(temp_dir, "file1.txt")
        file2_path = os.path.join(temp_dir, "file2.txt")

        with open(file1_path, "w") as f:
            f.write("Original content 1")
        with open(file2_path, "w") as f:
            f.write("Original content 2")

        # Create packet
        packet = {"input": file1_path, "output": file2_path}

        # Get original hash
        original_hash = hash_packet(packet)

        # Modify content of one file
        with open(file1_path, "w") as f:
            f.write("Modified content 1")

        # Hash should change
        modified_hash = hash_packet(packet)
        assert modified_hash != original_hash, "Hash should change when content changes"

        # Revert and modify the other file
        with open(file1_path, "w") as f:
            f.write("Original content 1")
        with open(file2_path, "w") as f:
            f.write("Modified content 2")

        # Hash should also change
        modified_hash2 = hash_packet(packet)
        assert modified_hash2 != original_hash, (
            "Hash should change when content changes"
        )
        assert modified_hash2 != modified_hash, (
            "Different modifications should yield different hashes"
        )


def test_hash_packet_structure_changes():
    """Test that hash_packet changes when packet structure changes."""
    # Create temp directory with files
    with tempfile.TemporaryDirectory() as temp_dir:
        file1_path = os.path.join(temp_dir, "file1.txt")
        file2_path = os.path.join(temp_dir, "file2.txt")
        file3_path = os.path.join(temp_dir, "file3.txt")

        with open(file1_path, "w") as f:
            f.write("Content 1")
        with open(file2_path, "w") as f:
            f.write("Content 2")
        with open(file3_path, "w") as f:
            f.write("Content 3")

        # Create original packet
        packet1 = {"input": file1_path, "output": file2_path}

        # Create packet with different keys
        packet2 = {"source": file1_path, "result": file2_path}

        # Create packet with additional file
        packet3 = {"input": file1_path, "output": file2_path, "extra": file3_path}

        # Get hashes
        hash1 = hash_packet(packet1)
        hash2 = hash_packet(packet2)
        hash3 = hash_packet(packet3)

        # All hashes should be different
        assert hash1 != hash2, "Different keys should produce different hashes"
        assert hash1 != hash3, "Additional entries should change the hash"
        assert hash2 != hash3, (
            "Different packet structures should have different hashes"
        )


if __name__ == "__main__":
    pytest.main(["-v", __file__])
