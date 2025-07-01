#!/usr/bin/env python
"""Tests for HasherFactory methods."""

import tempfile
from pathlib import Path

from orcapod.hashing.file_hashers import (
    LegacyDefaultFileHasher,
    LegacyCachedFileHasher,
    LegacyPathLikeHasherFactory,
)
from orcapod.hashing.string_cachers import FileCacher, InMemoryCacher


class TestPathLikeHasherFactoryCreateFileHasher:
    """Test cases for PathLikeHasherFactory.create_file_hasher method."""

    def test_create_file_hasher_without_cacher(self):
        """Test creating a file hasher without string cacher (returns BasicFileHasher)."""
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher()

        # Should return LegacyDefaultFileHasher
        assert isinstance(hasher, LegacyDefaultFileHasher)
        assert not isinstance(hasher, LegacyCachedFileHasher)

        # Check default parameters
        assert hasher.algorithm == "sha256"
        assert hasher.buffer_size == 65536

    def test_create_file_hasher_with_cacher(self):
        """Test creating a file hasher with string cacher (returns CachedFileHasher)."""
        cacher = InMemoryCacher()
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher
        )

        # Should return LegacyCachedFileHasher
        assert isinstance(hasher, LegacyCachedFileHasher)
        assert hasher.string_cacher is cacher

        # The underlying file hasher should be LegacyDefaultFileHasher with defaults
        assert isinstance(hasher.file_hasher, LegacyDefaultFileHasher)
        assert hasher.file_hasher.algorithm == "sha256"
        assert hasher.file_hasher.buffer_size == 65536

    def test_create_file_hasher_custom_algorithm(self):
        """Test creating file hasher with custom algorithm."""
        # Without cacher
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(algorithm="md5")
        assert isinstance(hasher, LegacyDefaultFileHasher)
        assert hasher.algorithm == "md5"
        assert hasher.buffer_size == 65536

        # With cacher
        cacher = InMemoryCacher()
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, algorithm="sha512"
        )
        assert isinstance(hasher, LegacyCachedFileHasher)
        assert isinstance(hasher.file_hasher, LegacyDefaultFileHasher)
        assert hasher.file_hasher.algorithm == "sha512"
        assert hasher.file_hasher.buffer_size == 65536

    def test_create_file_hasher_custom_buffer_size(self):
        """Test creating file hasher with custom buffer size."""
        # Without cacher
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            buffer_size=32768
        )
        assert isinstance(hasher, LegacyDefaultFileHasher)
        assert hasher.algorithm == "sha256"
        assert hasher.buffer_size == 32768

        # With cacher
        cacher = InMemoryCacher()
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, buffer_size=8192
        )
        assert isinstance(hasher, LegacyCachedFileHasher)
        assert isinstance(hasher.file_hasher, LegacyDefaultFileHasher)
        assert hasher.file_hasher.algorithm == "sha256"
        assert hasher.file_hasher.buffer_size == 8192

    def test_create_file_hasher_all_custom_parameters(self):
        """Test creating file hasher with all custom parameters."""
        cacher = InMemoryCacher(max_size=500)
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, algorithm="blake2b", buffer_size=16384
        )

        assert isinstance(hasher, LegacyCachedFileHasher)
        assert hasher.string_cacher is cacher
        assert isinstance(hasher.file_hasher, LegacyDefaultFileHasher)
        assert hasher.file_hasher.algorithm == "blake2b"
        assert hasher.file_hasher.buffer_size == 16384

    def test_create_file_hasher_different_cacher_types(self):
        """Test creating file hasher with different types of string cachers."""
        # InMemoryCacher
        memory_cacher = InMemoryCacher()
        hasher1 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=memory_cacher
        )
        assert isinstance(hasher1, LegacyCachedFileHasher)
        assert hasher1.string_cacher is memory_cacher

        # FileCacher
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            file_cacher = FileCacher(tmp_file.name)
            hasher2 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
                string_cacher=file_cacher
            )
            assert isinstance(hasher2, LegacyCachedFileHasher)
            assert hasher2.string_cacher is file_cacher

            # Clean up
            Path(tmp_file.name).unlink(missing_ok=True)

    def test_create_file_hasher_functional_without_cache(self):
        """Test that created file hasher actually works for hashing files."""
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            algorithm="sha256", buffer_size=1024
        )

        # Create a temporary file to hash
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write("Hello, World!")
            tmp_path = Path(tmp_file.name)

        try:
            # Hash the file
            hash_result = hasher.hash_file(tmp_path)

            # Verify it's a valid hash string
            assert isinstance(hash_result, str)
            assert len(hash_result) == 64  # SHA256 hex length
            assert all(c in "0123456789abcdef" for c in hash_result)

            # Hash the same file again - should get same result
            hash_result2 = hasher.hash_file(tmp_path)
            assert hash_result == hash_result2
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_create_file_hasher_functional_with_cache(self):
        """Test that created cached file hasher works and caches results."""
        cacher = InMemoryCacher()
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, algorithm="sha256"
        )

        # Create a temporary file to hash
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write("Test content for caching")
            tmp_path = Path(tmp_file.name)

        try:
            # First hash - should compute and cache
            hash_result1 = hasher.hash_file(tmp_path)
            assert isinstance(hash_result1, str)
            assert len(hash_result1) == 64

            # Verify it was cached
            cache_key = f"file:{tmp_path}"
            cached_value = cacher.get_cached(cache_key)
            assert cached_value == hash_result1

            # Second hash - should return cached value
            hash_result2 = hasher.hash_file(tmp_path)
            assert hash_result2 == hash_result1
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_create_file_hasher_none_cacher_explicit(self):
        """Test explicitly passing None for string_cacher."""
        hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=None, algorithm="sha1", buffer_size=4096
        )

        assert isinstance(hasher, LegacyDefaultFileHasher)
        assert not isinstance(hasher, LegacyCachedFileHasher)
        assert hasher.algorithm == "sha1"
        assert hasher.buffer_size == 4096

    def test_create_file_hasher_parameter_edge_cases(self):
        """Test edge cases for parameters."""
        # Very small buffer size
        hasher1 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(buffer_size=1)
        assert isinstance(hasher1, LegacyDefaultFileHasher)
        assert hasher1.buffer_size == 1

        # Large buffer size
        hasher2 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            buffer_size=1024 * 1024
        )
        assert isinstance(hasher2, LegacyDefaultFileHasher)
        assert hasher2.buffer_size == 1024 * 1024

        # Different algorithms
        for algorithm in ["md5", "sha1", "sha224", "sha256", "sha384", "sha512"]:
            hasher = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
                algorithm=algorithm
            )
            assert isinstance(hasher, LegacyDefaultFileHasher)
            assert hasher.algorithm == algorithm

    def test_create_file_hasher_cache_independence(self):
        """Test that different cached hashers with same cacher are independent."""
        cacher = InMemoryCacher()

        hasher1 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, algorithm="sha256"
        )
        hasher2 = LegacyPathLikeHasherFactory.create_legacy_file_hasher(
            string_cacher=cacher, algorithm="md5"
        )

        # Both should use the same cacher but be different instances
        assert isinstance(hasher1, LegacyCachedFileHasher)
        assert isinstance(hasher2, LegacyCachedFileHasher)
        assert hasher1.string_cacher is cacher
        assert hasher2.string_cacher is cacher
        assert hasher1 is not hasher2
        assert hasher1.file_hasher is not hasher2.file_hasher
        assert isinstance(hasher1.file_hasher, LegacyDefaultFileHasher)
        assert isinstance(hasher2.file_hasher, LegacyDefaultFileHasher)
        assert hasher1.file_hasher.algorithm != hasher2.file_hasher.algorithm
