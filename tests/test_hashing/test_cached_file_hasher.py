#!/usr/bin/env python
"""Tests for CachedFileHasher implementation."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from orcapod.hashing.file_hashers import (
    LegacyDefaultFileHasher,
    LegacyCachedFileHasher,
)
from orcapod.hashing.string_cachers import InMemoryCacher
from orcapod.hashing.types import LegacyFileHasher, StringCacher


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


def test_cached_file_hasher_construction():
    """Test that CachedFileHasher can be constructed with various parameters."""
    # Test with default parameters
    file_hasher = LegacyDefaultFileHasher()
    string_cacher = InMemoryCacher()

    cached_hasher1 = LegacyCachedFileHasher(file_hasher, string_cacher)
    assert cached_hasher1.file_hasher == file_hasher
    assert cached_hasher1.string_cacher == string_cacher

    # Test that CachedFileHasher implements FileHasher protocol
    assert isinstance(cached_hasher1, LegacyFileHasher)


def test_cached_file_hasher_file_caching():
    """Test that CachedFileHasher properly caches file hashing results."""
    # Get a sample file
    hash_lut = load_hash_lut()
    if not hash_lut:
        pytest.skip("No files in hash lookup table")

    filename, info = next(iter(hash_lut.items()))
    file_path = verify_path_exists(info["file"])
    expected_hash = info["hash"]

    # Create mock objects for testing
    mock_string_cacher = MagicMock(spec=StringCacher)
    mock_string_cacher.get_cached.return_value = None  # Initially no cached value

    file_hasher = LegacyDefaultFileHasher()
    cached_hasher = LegacyCachedFileHasher(file_hasher, mock_string_cacher)

    # First call should compute the hash and cache it
    result1 = cached_hasher.hash_file(file_path)
    assert result1 == expected_hash

    # Verify cache interaction
    cache_key = f"file:{file_path}"
    mock_string_cacher.get_cached.assert_called_once_with(cache_key)
    mock_string_cacher.set_cached.assert_called_once_with(cache_key, expected_hash)

    # Reset mock for second call
    mock_string_cacher.reset_mock()
    mock_string_cacher.get_cached.return_value = expected_hash  # Now it's cached

    # Second call should use the cached value
    result2 = cached_hasher.hash_file(file_path)
    assert result2 == expected_hash

    # Verify cache was checked but hash function wasn't called again
    mock_string_cacher.get_cached.assert_called_once_with(cache_key)
    mock_string_cacher.set_cached.assert_not_called()

    # Test with caching disabled
    mock_string_cacher.reset_mock()
    mock_string_cacher.get_cached.return_value = expected_hash


def test_cached_file_hasher_call_counts():
    """Test that the underlying file hasher is called only when needed with caching."""
    # Create a test file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"Test content for hashing")

    try:
        # Mock the file_hasher to track calls
        mock_file_hasher = MagicMock(spec=LegacyFileHasher)
        mock_file_hasher.hash_file.return_value = "mock_file_hash"

        # Real cacher
        string_cacher = InMemoryCacher()

        # Create the cached file hasher with all caching enabled
        cached_hasher = LegacyCachedFileHasher(
            mock_file_hasher,
            string_cacher,
        )

        # File hashing test
        file_path = temp_file.name

        # First call - should use the underlying hasher
        result1 = cached_hasher.hash_file(file_path)
        assert result1 == "mock_file_hash"
        mock_file_hasher.hash_file.assert_called_once_with(file_path)
        mock_file_hasher.hash_file.reset_mock()

        # Second call - should use cache
        result2 = cached_hasher.hash_file(file_path)
        assert result2 == "mock_file_hash"
        mock_file_hasher.hash_file.assert_not_called()

    finally:
        # Clean up the temporary file
        os.unlink(temp_file.name)


def test_cached_file_hasher_performance():
    """Test that caching improves performance for repeated hashing operations."""
    # This test is optional but can be useful to verify performance benefits
    import time

    # Get a sample file
    hash_lut = load_hash_lut()
    if not hash_lut:
        pytest.skip("No files in hash lookup table")

    filename, info = next(iter(hash_lut.items()))
    file_path = verify_path_exists(info["file"])

    # Setup non-cached hasher
    file_hasher = LegacyDefaultFileHasher()

    # Setup cached hasher
    string_cacher = InMemoryCacher()
    cached_hasher = LegacyCachedFileHasher(file_hasher, string_cacher)

    # Measure time for multiple hash operations with non-cached hasher
    start_time = time.time()
    for _ in range(5):
        file_hasher.hash_file(file_path)
    non_cached_time = time.time() - start_time

    # First call to cached hasher (not cached yet)
    cached_hasher.hash_file(file_path)

    # Measure time for multiple hash operations with cached hasher
    start_time = time.time()
    for _ in range(5):
        cached_hasher.hash_file(file_path)
    cached_time = time.time() - start_time

    # The cached version should be faster, but we don't assert specific times
    # as they depend on the environment
    print(f"Non-cached: {non_cached_time:.6f}s, Cached: {cached_time:.6f}s")

    # If for some reason caching is slower, this test would fail,
    # which might indicate a problem with the implementation
    # But we're not making this assertion because timing tests can be unreliable
    assert cached_time < non_cached_time


def test_cached_file_hasher_with_different_cachers():
    """Test CachedFileHasher works with different StringCacher implementations."""

    # Create a test file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"Test content for hashing")

    try:
        file_path = temp_file.name
        file_hasher = LegacyDefaultFileHasher()

        # Test with InMemoryCacher
        mem_cacher = InMemoryCacher(max_size=10)
        cached_hasher1 = LegacyCachedFileHasher(file_hasher, mem_cacher)

        # First hash call
        hash1 = cached_hasher1.hash_file(file_path)

        # Check that it was cached
        cached_value = mem_cacher.get_cached(f"file:{file_path}")
        assert cached_value == hash1

        # Create a custom StringCacher
        class CustomCacher(StringCacher):
            def __init__(self):
                self.storage = {}

            def get_cached(self, cache_key: str) -> str | None:
                return self.storage.get(cache_key)

            def set_cached(self, cache_key: str, value: str) -> None:
                self.storage[cache_key] = f"CUSTOM_{value}"

            def clear_cache(self) -> None:
                self.storage.clear()

        custom_cacher = CustomCacher()
        cached_hasher2 = LegacyCachedFileHasher(file_hasher, custom_cacher)

        # Get hash with custom cacher
        hash2 = cached_hasher2.hash_file(file_path)

        # Check the custom cacher modified the stored value
        cached_value = custom_cacher.get_cached(f"file:{file_path}")
        assert cached_value == f"CUSTOM_{hash2}"

        # But the returned hash should be the original, unmodified hash
        assert hash1 == hash2

    finally:
        # Clean up the temporary file
        os.unlink(temp_file.name)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
