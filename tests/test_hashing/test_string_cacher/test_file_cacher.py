"""Tests for FileCacher."""

import json
import tempfile
import threading
from pathlib import Path
from unittest.mock import mock_open, patch

from orcapod.hashing.string_cachers import FileCacher


def test_basic_operations():
    """Test basic get/set/clear operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "test_cache.json"
        cacher = FileCacher(cache_file, sync_probability=1.0)  # Always sync

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


def test_persistence_across_instances():
    """Test that data persists across different cacher instances."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "persistent_cache.json"

        # First instance
        cacher1 = FileCacher(cache_file, sync_probability=1.0)
        cacher1.set_cached("key1", "value1")
        cacher1.set_cached("key2", "value2")
        cacher1.force_sync()  # Ensure data is written

        # Second instance should load existing data
        cacher2 = FileCacher(cache_file)
        assert cacher2.get_cached("key1") == "value1"
        assert cacher2.get_cached("key2") == "value2"

        # Add more data in second instance
        cacher2.set_cached("key3", "value3")
        cacher2.force_sync()

        # Third instance should see all data
        cacher3 = FileCacher(cache_file)
        assert cacher3.get_cached("key1") == "value1"
        assert cacher3.get_cached("key2") == "value2"
        assert cacher3.get_cached("key3") == "value3"


def test_file_loading_on_init():
    """Test loading existing file data on initialization."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "preexisting_cache.json"

        # Create file with existing data
        initial_data = {
            "cache": {"existing_key": "existing_value"},
            "access_order": ["existing_key"],
        }

        with open(cache_file, "w") as f:
            json.dump(initial_data, f)

        # Initialize cacher - should load existing data
        cacher = FileCacher(cache_file)
        assert cacher.get_cached("existing_key") == "existing_value"


def test_corrupted_file_handling():
    """Test handling of corrupted JSON files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "corrupted_cache.json"

        # Create corrupted JSON file
        with open(cache_file, "w") as f:
            f.write("{ invalid json content")

        # Should handle corruption gracefully
        with patch("logging.warning") as mock_log:
            cacher = FileCacher(cache_file)
            mock_log.assert_called_once()

        # Should start with empty cache
        assert cacher.get_cached("any_key") is None

        # Should still be able to operate normally
        cacher.set_cached("new_key", "new_value")
        assert cacher.get_cached("new_key") == "new_value"


def test_nonexistent_file_handling():
    """Test handling when cache file doesn't exist initially."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "nonexistent_cache.json"

        # File doesn't exist
        assert not cache_file.exists()

        # Should initialize successfully
        cacher = FileCacher(cache_file)
        assert cacher.get_cached("any_key") is None

        # Should create file on first sync
        cacher.set_cached("key1", "value1")
        cacher.force_sync()
        assert cache_file.exists()


def test_sync_probability():
    """Test probabilistic syncing behavior."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "prob_cache.json"

        # Test with 0% sync probability
        cacher = FileCacher(cache_file, sync_probability=0.0)
        cacher.set_cached("key1", "value1")

        # File should not be created (no sync)
        assert not cache_file.exists()

        # Force sync should still work
        cacher.force_sync()
        assert cache_file.exists()

        # Test with 100% sync probability
        cache_file2 = Path(temp_dir) / "always_sync_cache.json"
        cacher2 = FileCacher(cache_file2, sync_probability=1.0)
        cacher2.set_cached("key1", "value1")

        # File should be created immediately
        assert cache_file2.exists()


def test_lru_behavior():
    """Test LRU eviction behavior."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "lru_cache.json"
        cacher = FileCacher(cache_file, max_size=3, sync_probability=1.0)

        # Fill cache to capacity
        cacher.set_cached("key1", "value1")
        cacher.set_cached("key2", "value2")
        cacher.set_cached("key3", "value3")

        # Add one more - should evict oldest
        cacher.set_cached("key4", "value4")

        assert cacher.get_cached("key1") is None  # Evicted
        assert cacher.get_cached("key2") == "value2"
        assert cacher.get_cached("key3") == "value3"
        assert cacher.get_cached("key4") == "value4"


def test_atomic_file_writes():
    """Test that file writes are atomic (temp file + rename)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "atomic_cache.json"

        # Mock the rename operation to fail
        with patch("pathlib.Path.replace") as mock_replace:
            mock_replace.side_effect = OSError("Simulated failure")

            cacher = FileCacher(cache_file, sync_probability=1.0)

            with patch("logging.error") as mock_log:
                cacher.set_cached("key1", "value1")  # Should trigger sync
                mock_log.assert_called_once()

            # Original file should not exist due to failed rename
            assert not cache_file.exists()


def test_directory_creation():
    """Test that parent directories are created if they don't exist."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create nested path that doesn't exist
        cache_file = Path(temp_dir) / "nested" / "dirs" / "cache.json"

        cacher = FileCacher(cache_file, sync_probability=1.0)
        cacher.set_cached("key1", "value1")

        # File and directories should be created
        assert cache_file.exists()
        assert cache_file.parent.exists()


def test_thread_safety():
    """Test thread safety of file operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "thread_safe_cache.json"
        cacher = FileCacher(cache_file, max_size=100, sync_probability=0.1)

        results = {}
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(20):
                    key = f"thread{thread_id}_key{i}"
                    value = f"thread{thread_id}_value{i}"
                    cacher.set_cached(key, value)

                    # Occasionally force sync
                    if i % 5 == 0:
                        cacher.force_sync()

                # Verify data
                thread_results = []
                for i in range(20):
                    key = f"thread{thread_id}_key{i}"
                    result = cacher.get_cached(key)
                    thread_results.append(result)

                results[thread_id] = thread_results

            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Check for errors
        assert not errors, f"Thread safety errors: {errors}"

        # Final sync to ensure persistence
        cacher.force_sync()


def test_force_sync():
    """Test explicit force_sync method."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "force_sync_cache.json"

        # Use 0% sync probability so only force_sync writes
        cacher = FileCacher(cache_file, sync_probability=0.0)
        cacher.set_cached("key1", "value1")

        # File shouldn't exist yet
        assert not cache_file.exists()

        # Force sync
        cacher.force_sync()
        assert cache_file.exists()

        # Verify content
        with open(cache_file) as f:
            data = json.load(f)
            assert data["cache"]["key1"] == "value1"


def test_access_order_persistence():
    """Test that access order is persisted and restored correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "access_order_cache.json"

        # First instance - create data with specific access pattern
        cacher1 = FileCacher(cache_file, sync_probability=1.0)
        cacher1.set_cached("key1", "value1")  # Added first
        cacher1.set_cached("key2", "value2")  # Added second
        cacher1.set_cached("key3", "value3")  # Added third

        # Access key1 to change order
        cacher1.get_cached("key1")  # key1 becomes most recent
        cacher1.force_sync()

        # Second instance - should maintain access order
        cacher2 = FileCacher(cache_file, max_size=2)  # Limit size to test eviction

        # Add new item - should evict key2 (oldest unaccessed)
        cacher2.set_cached("key4", "value4")

        assert cacher2.get_cached("key1") == "value1"  # Still there (recently accessed)
        assert cacher2.get_cached("key2") is None  # Evicted
        assert cacher2.get_cached("key3") == "value3"  # Still there
        assert cacher2.get_cached("key4") == "value4"  # New item


def test_file_io_error_handling():
    """Test handling of various file I/O errors."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_file = Path(temp_dir) / "io_error_cache.json"

        # Test write permission error
        with patch("builtins.open", mock_open()) as mock_file:
            mock_file.side_effect = PermissionError("Access denied")

            with patch("logging.error") as mock_log:
                cacher = FileCacher(cache_file, sync_probability=1.0)
                cacher.set_cached("key1", "value1")  # Should trigger failed sync
                mock_log.assert_called_once()

        # Test read error during initialization
        with patch("builtins.open", mock_open()) as mock_file:
            mock_file.side_effect = IOError("Read error")

            with patch.object(Path, "exists", return_value=True):
                with patch("logging.warning") as mock_log:
                    cacher = FileCacher(cache_file)
                    mock_log.assert_called_once()
                    assert cacher.get_cached("any_key") is None
