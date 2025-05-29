"""Tests for string cacher implementations."""

import pytest
from orcabridge.hashing.string_cachers import InMemoryCacher


def test_in_memory_cacher_basic_functionality():
    """Test basic InMemoryCacher functionality."""
    cacher = InMemoryCacher()
    
    # Test set and get
    cacher.set_cached("key1", "value1")
    assert cacher.get_cached("key1") == "value1"
    
    # Test updating existing key
    cacher.set_cached("key1", "updated_value1")
    assert cacher.get_cached("key1") == "updated_value1"
    
    # Test non-existent key
    assert cacher.get_cached("non_existent_key") is None
    
    # Test clear cache
    cacher.clear_cache()
    assert cacher.get_cached("key1") is None


def test_in_memory_cacher_unlimited_size():
    """Test that InMemoryCacher with max_size=None can hold large number of items."""
    cacher = InMemoryCacher(max_size=None)
    
    # Add more than 1000 items to ensure it can handle a large number
    for i in range(1500):
        key = f"key{i}"
        value = f"value{i}"
        cacher.set_cached(key, value)
    
    # Verify all items are still in the cache
    for i in range(1500):
        key = f"key{i}"
        expected_value = f"value{i}"
        assert cacher.get_cached(key) == expected_value, f"Item {key} missing or incorrect"
    
    # Verify the cache size is correct
    assert len(cacher._cache) == 1500


def test_in_memory_cacher_lru_eviction():
    """Test that LRU eviction works correctly with limited cache size."""
    # Create a cacher with small max_size for testing
    cacher = InMemoryCacher(max_size=3)
    
    # Add initial items
    cacher.set_cached("key1", "value1")
    cacher.set_cached("key2", "value2")
    cacher.set_cached("key3", "value3")
    
    # All three items should be in the cache
    assert cacher.get_cached("key1") == "value1"
    assert cacher.get_cached("key2") == "value2"
    assert cacher.get_cached("key3") == "value3"
    
    # Access key1 to move it to the end of the LRU order (most recently used)
    cacher.get_cached("key1")
    
    # Add a new item, which should evict key2 (the least recently used)
    cacher.set_cached("key4", "value4")
    
    # key2 should be evicted
    assert cacher.get_cached("key2") is None
    
    # Other items should still be in the cache
    assert cacher.get_cached("key1") == "value1"
    assert cacher.get_cached("key3") == "value3"
    assert cacher.get_cached("key4") == "value4"
    
    # Accessing key3, then key1, then key4 makes key1 the middle item in recency
    cacher.get_cached("key3")
    cacher.get_cached("key1")
    cacher.get_cached("key4")
    
    # Add a new item, which should evict key3 (now the least recently used)
    cacher.set_cached("key5", "value5")
    
    # key3 should be evicted
    assert cacher.get_cached("key3") is None
    
    # key1, key4, key5 should remain
    assert cacher.get_cached("key1") == "value1"
    assert cacher.get_cached("key4") == "value4"
    assert cacher.get_cached("key5") == "value5"


def test_thread_safety():
    """Test basic thread safety properties."""
    # This is a simplified test that ensures no exceptions occur
    # For thorough thread safety testing, more complex test patterns would be needed
    import threading
    import random
    
    cacher = InMemoryCacher(max_size=50)
    errors = []
    
    def worker(worker_id, iterations=100):
        try:
            for i in range(iterations):
                operation = random.randint(0, 2)
                key = f"key{random.randint(0, 99)}"
                
                if operation == 0:  # get
                    cacher.get_cached(key)
                elif operation == 1:  # set
                    cacher.set_cached(key, f"value-{worker_id}-{i}")
                else:  # clear (less frequently)
                    if random.random() < 0.1:  # 10% chance to clear
                        cacher.clear_cache()
        except Exception as e:
            errors.append(f"Error in worker {worker_id}: {str(e)}")
    
    # Create and start multiple threads
    threads = []
    for i in range(5):  # 5 concurrent threads
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Check if any errors occurred
    assert not errors, f"Thread safety errors: {errors}"
