#!/usr/bin/env python
# filepath: /home/eywalker/workspace/orcabridge/tests/test_hashing/test_path_set_hasher.py
"""Tests for the PathSetHasher protocol implementation."""

import pytest
import os
import tempfile
from pathlib import Path

from orcabridge.hashing.file_hashers import DefaultPathsetHasher
from orcabridge.types import PathSet
from orcabridge.hashing.types import FileHasher


class MockFileHasher(FileHasher):
    """Simple mock FileHasher for testing."""

    def __init__(self, hash_value="mock_hash"):
        self.hash_value = hash_value
        self.file_hash_calls = []

    def hash_file(self, file_path):
        self.file_hash_calls.append(file_path)
        return f"{self.hash_value}_{file_path}"


def create_temp_file(content="test content"):
    """Create a temporary file for testing."""
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def test_default_pathset_hasher_single_file():
    """Test DefaultPathsetHasher with a single file path."""
    file_hasher = MockFileHasher()
    pathset_hasher = DefaultPathsetHasher(file_hasher)

    # Create a real file for testing
    file_path = create_temp_file()
    try:
        # Test with a single file path
        pathset = file_path

        result = pathset_hasher.hash_pathset(pathset)

        # Verify the file_hasher was called with the correct path
        assert len(file_hasher.file_hash_calls) == 1
        assert str(file_hasher.file_hash_calls[0]) == file_path

        # The result should be a string hash
        assert isinstance(result, str)
    finally:
        os.remove(file_path)


def test_default_pathset_hasher_multiple_files():
    """Test DefaultPathsetHasher with multiple files in a list."""
    file_hasher = MockFileHasher()
    pathset_hasher = DefaultPathsetHasher(file_hasher)

    # Create real files for testing
    file_paths = [create_temp_file(f"content {i}") for i in range(3)]
    try:
        pathset = file_paths

        result = pathset_hasher.hash_pathset(pathset)

        # Verify the file_hasher was called for each file
        assert len(file_hasher.file_hash_calls) == 3
        for i, path in enumerate(file_paths):
            assert str(file_hasher.file_hash_calls[i]) == path

        # The result should be a string hash
        assert isinstance(result, str)
    finally:
        for path in file_paths:
            os.remove(path)


def test_default_pathset_hasher_nested_paths():
    """Test DefaultPathsetHasher with nested path structures."""
    file_hasher = MockFileHasher()
    pathset_hasher = DefaultPathsetHasher(file_hasher)

    # Create temp files and a temp directory
    temp_dir = tempfile.mkdtemp()
    file1 = create_temp_file("file1 content")
    file2 = create_temp_file("file2 content")
    file3 = create_temp_file("file3 content")

    try:
        # Test with nested path structure using real paths
        nested_pathset = {
            "dir1": [file1, file2],
            "dir2": {"subdir": [file3]},
        }

        result = pathset_hasher.hash_pathset(nested_pathset)

        # Verify all files were hashed (3 in total)
        assert len(file_hasher.file_hash_calls) == 3
        assert file1 in [str(call) for call in file_hasher.file_hash_calls]
        assert file2 in [str(call) for call in file_hasher.file_hash_calls]
        assert file3 in [str(call) for call in file_hasher.file_hash_calls]

        # The result should be a string hash
        assert isinstance(result, str)
    finally:
        os.remove(file1)
        os.remove(file2)
        os.remove(file3)
        os.rmdir(temp_dir)


def test_default_pathset_hasher_with_nonexistent_files():
    """Test DefaultPathsetHasher with both existent and non-existent files."""
    file_hasher = MockFileHasher()
    pathset_hasher = DefaultPathsetHasher(file_hasher)

    # Create a real file for testing
    real_file = create_temp_file("real file content")
    try:
        # For testing nonexistent files, we'll modify the hash_file method to handle nonexistent files
        original_hash_file = file_hasher.hash_file

        def patched_hash_file(file_path):
            # Add to call list but don't check existence
            file_hasher.file_hash_calls.append(file_path)
            return f"{file_hasher.hash_value}_{file_path}"

        file_hasher.hash_file = patched_hash_file

        # Mix of existent and non-existent paths
        nonexistent_path = "/path/to/nonexistent.txt"  # This doesn't need to exist with our patched function
        pathset = [real_file, nonexistent_path]

        # We need to modify the DefaultPathsetHasher to use our mocked hasher
        pathset_hasher.file_hasher = file_hasher

        result = pathset_hasher.hash_pathset(pathset)

        # Verify all paths were passed to the file hasher
        assert len(file_hasher.file_hash_calls) == 2
        assert str(file_hasher.file_hash_calls[0]) == real_file
        assert str(file_hasher.file_hash_calls[1]) == nonexistent_path

        # The result should still be a string hash
        assert isinstance(result, str)

        # Restore original hash_file method
        file_hasher.hash_file = original_hash_file
    finally:
        os.remove(real_file)


def test_default_pathset_hasher_with_char_count():
    """Test DefaultPathsetHasher with different char_count values."""
    file_hasher = MockFileHasher()

    # Create a real file for testing
    file_path = create_temp_file("char count test content")

    try:
        # Test with default char_count (32)
        default_hasher = DefaultPathsetHasher(file_hasher)
        default_result = default_hasher.hash_pathset(file_path)

        # Reset call list
        file_hasher.file_hash_calls = []

        # Test with custom char_count
        custom_hasher = DefaultPathsetHasher(file_hasher, char_count=16)
        custom_result = custom_hasher.hash_pathset(file_path)

        # Both should have called the file_hasher once
        assert len(file_hasher.file_hash_calls) == 1

        # Both results should be strings
        assert isinstance(default_result, str)
        assert isinstance(custom_result, str)
    finally:
        os.remove(file_path)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
