"""
Test module for CacheStream mapper.

This module tests the CacheStream mapper functionality, which provides
caching capabilities to avoid upstream recomputation by storing stream data
in memory after the first iteration.
"""

import pytest
from unittest.mock import Mock

from orcabridge.base import SyncStream
from orcabridge.mapper import CacheStream
from orcabridge.stream import SyncStreamFromLists


@pytest.fixture
def cache_mapper():
    """Create a CacheStream mapper instance."""
    return CacheStream()


@pytest.fixture
def sample_stream_data():
    """Sample stream data for testing."""
    return [
        ({"id": 1}, {"value": 10}),
        ({"id": 2}, {"value": 20}),
        ({"id": 3}, {"value": 30}),
    ]


@pytest.fixture
def sample_stream(sample_stream_data):
    """Create a sample stream."""
    tags, packets = zip(*sample_stream_data)
    return SyncStreamFromLists(list(tags), list(packets))


class TestCacheStream:
    """Test cases for CacheStream mapper."""

    def test_cache_initialization(self, cache_mapper):
        """Test that CacheStream initializes with empty cache."""
        assert cache_mapper.cache == []
        assert cache_mapper.is_cached is False

    def test_repr(self, cache_mapper):
        """Test CacheStream string representation."""
        assert repr(cache_mapper) == "CacheStream(active:False)"

        # After caching
        cache_mapper.is_cached = True
        assert repr(cache_mapper) == "CacheStream(active:True)"

    def test_first_iteration_caches_data(self, cache_mapper, sample_stream):
        """Test that first iteration through stream caches the data."""
        cached_stream = cache_mapper(sample_stream)

        # Initially not cached
        assert not cache_mapper.is_cached
        assert len(cache_mapper.cache) == 0

        # Iterate through stream
        result = list(cached_stream)

        # After iteration, should be cached
        assert cache_mapper.is_cached
        assert len(cache_mapper.cache) == 3
        assert cache_mapper.cache == [
            ({"id": 1}, {"value": 10}),
            ({"id": 2}, {"value": 20}),
            ({"id": 3}, {"value": 30}),
        ]

        # Result should match original stream
        assert result == [
            ({"id": 1}, {"value": 10}),
            ({"id": 2}, {"value": 20}),
            ({"id": 3}, {"value": 30}),
        ]

    def test_subsequent_iterations_use_cache(self, cache_mapper, sample_stream):
        """Test that subsequent iterations use cached data."""
        cached_stream = cache_mapper(sample_stream)

        # First iteration
        first_result = list(cached_stream)
        assert cache_mapper.is_cached

        # Create new stream from same mapper (simulates reuse)
        second_cached_stream = cache_mapper()  # No input streams for cached version
        second_result = list(second_cached_stream)

        # Results should be identical
        assert first_result == second_result
        assert second_result == [
            ({"id": 1}, {"value": 10}),
            ({"id": 2}, {"value": 20}),
            ({"id": 3}, {"value": 30}),
        ]

    def test_clear_cache(self, cache_mapper, sample_stream):
        """Test cache clearing functionality."""
        cached_stream = cache_mapper(sample_stream)

        # Cache some data
        list(cached_stream)
        assert cache_mapper.is_cached
        assert len(cache_mapper.cache) > 0

        # Clear cache
        cache_mapper.clear_cache()
        assert not cache_mapper.is_cached
        assert len(cache_mapper.cache) == 0

    def test_multiple_streams_error_when_not_cached(self, cache_mapper, sample_stream):
        """Test that providing multiple streams raises error when not cached."""
        stream2 = SyncStreamFromLists([{"id": 4}], [{"value": 40}])

        with pytest.raises(
            ValueError, match="CacheStream operation requires exactly one stream"
        ):
            cache_mapper(sample_stream, stream2)

    def test_no_streams_when_cached(self, cache_mapper, sample_stream):
        """Test that cached stream can be called without input streams."""
        # First, cache some data
        cached_stream = cache_mapper(sample_stream)
        list(cached_stream)  # This caches the data

        # Now call without streams (should use cache)
        cached_only_stream = cache_mapper()
        result = list(cached_only_stream)

        assert result == [
            ({"id": 1}, {"value": 10}),
            ({"id": 2}, {"value": 20}),
            ({"id": 3}, {"value": 30}),
        ]

    def test_empty_stream_caching(self, cache_mapper):
        """Test caching behavior with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        cached_stream = cache_mapper(empty_stream)

        result = list(cached_stream)

        assert result == []
        assert cache_mapper.is_cached
        assert cache_mapper.cache == []

    def test_identity_structure(self, cache_mapper, sample_stream):
        """Test that CacheStream has unique identity structure."""
        # CacheStream should return None for identity structure
        # to treat every instance as different
        assert cache_mapper.identity_structure(sample_stream) is None

    def test_avoids_upstream_recomputation(self, cache_mapper):
        """Test that CacheStream avoids upstream recomputation."""
        # Create a mock stream that tracks how many times it's iterated
        iteration_count = {"count": 0}

        def counting_generator():
            iteration_count["count"] += 1
            yield ({"id": 1}, {"value": 10})
            yield ({"id": 2}, {"value": 20})

        mock_stream = Mock(spec=SyncStream)
        mock_stream.__iter__ = counting_generator

        cached_stream = cache_mapper(mock_stream)

        # First iteration should call upstream
        list(cached_stream)
        assert iteration_count["count"] == 1

        # Second iteration should use cache (not call upstream)
        second_cached_stream = cache_mapper()
        list(second_cached_stream)
        assert iteration_count["count"] == 1  # Should still be 1

    def test_cache_with_different_data_types(self, cache_mapper):
        """Test caching with various data types."""
        complex_data = [
            ({"id": 1, "type": "string"}, {"data": "hello", "numbers": [1, 2, 3]}),
            ({"id": 2, "type": "dict"}, {"data": {"nested": True}, "numbers": None}),
            ({"id": 3, "type": "boolean"}, {"data": True, "numbers": 42}),
        ]

        tags, packets = zip(*complex_data)
        stream = SyncStreamFromLists(list(tags), list(packets))
        cached_stream = cache_mapper(stream)

        result = list(cached_stream)

        assert result == complex_data
        assert cache_mapper.is_cached
        assert cache_mapper.cache == complex_data

    def test_multiple_cache_instances(self, sample_stream):
        """Test that different CacheStream instances have separate caches."""
        cache1 = CacheStream()
        cache2 = CacheStream()

        # Cache in first instance
        cached_stream1 = cache1(sample_stream)
        list(cached_stream1)

        # Second instance should not be cached
        assert cache1.is_cached
        assert not cache2.is_cached
        assert len(cache1.cache) == 3
        assert len(cache2.cache) == 0

    def test_keys_method(self, cache_mapper, sample_stream):
        """Test that CacheStream passes through keys correctly."""
        # CacheStream should inherit keys from input stream
        tag_keys, packet_keys = cache_mapper.keys(sample_stream)
        original_tag_keys, original_packet_keys = sample_stream.keys()

        assert tag_keys == original_tag_keys
        assert packet_keys == original_packet_keys

    def test_chaining_with_cache(self, cache_mapper, sample_stream):
        """Test chaining CacheStream with other operations."""
        from orcabridge.mapper import Filter

        # Chain cache with filter
        filter_mapper = Filter(lambda tag, packet: tag["id"] > 1)

        # Cache first, then filter
        cached_stream = cache_mapper(sample_stream)
        filtered_stream = filter_mapper(cached_stream)

        result = list(filtered_stream)

        assert len(result) == 2  # Should have filtered out id=1
        assert result == [
            ({"id": 2}, {"value": 20}),
            ({"id": 3}, {"value": 30}),
        ]

        # Cache should still be populated with original data
        assert cache_mapper.is_cached
        assert len(cache_mapper.cache) == 3

    def test_cache_persistence_across_multiple_outputs(
        self, cache_mapper, sample_stream
    ):
        """Test that cache persists when creating multiple output streams."""
        # First stream
        stream1 = cache_mapper(sample_stream)
        result1 = list(stream1)

        # Second stream from same cache
        stream2 = cache_mapper()
        result2 = list(stream2)

        # Third stream from same cache
        stream3 = cache_mapper()
        result3 = list(stream3)

        # All results should be identical
        assert result1 == result2 == result3
        assert len(result1) == 3

    def test_error_handling_during_caching(self, cache_mapper):
        """Test error handling when upstream stream raises exception."""

        def error_generator():
            yield ({"id": 1}, {"value": 10})
            raise ValueError("Upstream error")

        mock_stream = Mock(spec=SyncStream)
        mock_stream.__iter__ = error_generator

        cached_stream = cache_mapper(mock_stream)

        # Should propagate the error and not cache partial data
        with pytest.raises(ValueError, match="Upstream error"):
            list(cached_stream)

        # Cache should remain empty after error
        assert not cache_mapper.is_cached
        assert len(cache_mapper.cache) == 0

    def test_cache_stream_pickle(self):
        """Test that CacheStream mapper is pickleable."""
        import pickle
        from orcabridge.mappers import CacheStream

        cache_stream = CacheStream()
        pickled = pickle.dumps(cache_stream)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, CacheStream)
        assert unpickled.__class__.__name__ == "CacheStream"
