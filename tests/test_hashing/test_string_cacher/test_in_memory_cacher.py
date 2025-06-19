"""Tests for InMemoryCacher."""

import threading
import time

from orcapod.hashing.string_cachers import InMemoryCacher


def test_basic_operations():
    """Test basic get/set/clear operations."""
    cacher = InMemoryCacher()

    # Test empty cache
    assert cacher.get_cached("nonexistent") is None

    # Test set and get
    cacher.set_cached("key1", "value1")
    assert cacher.get_cached("key1") == "value1"

    # Test overwrite
    cacher.set_cached("key1", "new_value1")
    assert cacher.get_cached("key1") == "new_value1"

    # Test multiple keys
    cacher.set_cached("key2", "value2")
    assert cacher.get_cached("key1") == "new_value1"
    assert cacher.get_cached("key2") == "value2"

    # Test clear
    cacher.clear_cache()
    assert cacher.get_cached("key1") is None
    assert cacher.get_cached("key2") is None


def test_lru_eviction():
    """Test LRU eviction behavior."""
    cacher = InMemoryCacher(max_size=3)

    # Fill cache to capacity
    cacher.set_cached("key1", "value1")
    cacher.set_cached("key2", "value2")
    cacher.set_cached("key3", "value3")

    assert cacher.get_cached("key1") == "value1"
    assert cacher.get_cached("key2") == "value2"
    assert cacher.get_cached("key3") == "value3"

    # Add one more item - should evict oldest (key1)
    cacher.set_cached("key4", "value4")

    assert cacher.get_cached("key1") is None  # Evicted
    assert cacher.get_cached("key2") == "value2"
    assert cacher.get_cached("key3") == "value3"
    assert cacher.get_cached("key4") == "value4"


def test_lru_access_updates_order():
    """Test that accessing items updates their position in LRU order."""
    cacher = InMemoryCacher(max_size=3)

    # Fill cache
    cacher.set_cached("key1", "value1")
    cacher.set_cached("key2", "value2")
    cacher.set_cached("key3", "value3")

    # Access key1 to make it recently used
    cacher.get_cached("key1")

    # Add new item - should evict key2 (oldest unused)
    cacher.set_cached("key4", "value4")

    assert cacher.get_cached("key1") == "value1"  # Still there
    assert cacher.get_cached("key2") is None  # Evicted
    assert cacher.get_cached("key3") == "value3"
    assert cacher.get_cached("key4") == "value4"


def test_unlimited_size():
    """Test behavior with unlimited cache size."""
    cacher = InMemoryCacher(max_size=None)

    # Add many items
    for i in range(1000):
        cacher.set_cached(f"key{i}", f"value{i}")

    # All should still be accessible
    for i in range(1000):
        assert cacher.get_cached(f"key{i}") == f"value{i}"


def test_thread_safety():
    """Test thread safety of cache operations."""
    cacher = InMemoryCacher(max_size=100)
    results = {}
    errors = []

    def worker(thread_id: int):
        try:
            # Each thread writes and reads its own keys
            for i in range(50):
                key = f"thread{thread_id}_key{i}"
                value = f"thread{thread_id}_value{i}"
                cacher.set_cached(key, value)

            # Verify all keys
            thread_results = []
            for i in range(50):
                key = f"thread{thread_id}_key{i}"
                result = cacher.get_cached(key)
                thread_results.append(result)

            results[thread_id] = thread_results
        except Exception as e:
            errors.append(e)

    # Start multiple threads
    threads = []
    for i in range(5):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join()

    # Check for errors
    assert not errors, f"Thread safety errors: {errors}"

    # Verify each thread's results
    for thread_id in range(5):
        thread_results = results[thread_id]
        for i, result in enumerate(thread_results):
            expected = f"thread{thread_id}_value{i}"
            # Result might be None due to LRU eviction, but if present should be correct
            assert result is None or result == expected


def test_concurrent_access_same_key():
    """Test concurrent access to the same key."""
    cacher = InMemoryCacher()
    results = []
    errors = []

    def reader():
        try:
            for _ in range(100):
                result = cacher.get_cached("shared_key")
                results.append(result)
                time.sleep(0.001)  # Small delay
        except Exception as e:
            errors.append(e)

    def writer():
        try:
            for i in range(100):
                cacher.set_cached("shared_key", f"value{i}")
                time.sleep(0.001)  # Small delay
        except Exception as e:
            errors.append(e)

    # Start reader and writer threads
    reader_thread = threading.Thread(target=reader)
    writer_thread = threading.Thread(target=writer)

    reader_thread.start()
    writer_thread.start()

    reader_thread.join()
    writer_thread.join()

    # Should not have any errors
    assert not errors, f"Concurrent access errors: {errors}"

    # Results should be either None or valid values
    valid_values = {f"value{i}" for i in range(100)} | {None}
    for result in results:
        assert result in valid_values


def test_overwrite_existing_key_maintains_lru_order():
    """Test that overwriting an existing key maintains proper LRU order."""
    cacher = InMemoryCacher(max_size=3)

    # Fill cache
    cacher.set_cached("key1", "value1")
    cacher.set_cached("key2", "value2")
    cacher.set_cached("key3", "value3")

    # Overwrite middle key
    cacher.set_cached("key2", "new_value2")

    # Add new key - should evict key1 (oldest)
    cacher.set_cached("key4", "value4")

    assert cacher.get_cached("key1") is None  # Evicted
    assert len(cacher._access_order) == 3
    assert cacher.get_cached("key2") == "new_value2"  # Updated and moved to end
    assert cacher.get_cached("key3") == "value3"
    assert cacher.get_cached("key4") == "value4"


def test_empty_cache_operations():
    """Test operations on empty cache."""
    cacher = InMemoryCacher()

    # Get from empty cache
    assert cacher.get_cached("any_key") is None

    # Clear empty cache
    cacher.clear_cache()  # Should not raise

    # Verify still empty
    assert cacher.get_cached("any_key") is None


def test_edge_cases():
    """Test edge cases and boundary conditions."""
    # Test with max_size=1
    cacher = InMemoryCacher(max_size=1)
    cacher.set_cached("key1", "value1")
    assert cacher.get_cached("key1") == "value1"

    cacher.set_cached("key2", "value2")  # Should evict key1
    assert cacher.get_cached("key1") is None
    assert cacher.get_cached("key2") == "value2"

    # Test with max_size=0 (edge case)
    cacher = InMemoryCacher(max_size=0)
    cacher.set_cached("key1", "value1")  # Should immediately evict
    assert cacher.get_cached("key1") is None
