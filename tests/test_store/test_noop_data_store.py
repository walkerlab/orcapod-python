#!/usr/bin/env python
"""Tests for NoOpDataStore."""

import pytest

from orcapod.databases.legacy.dict_data_stores import NoOpDataStore


def test_noop_data_store_memoize():
    """Test that NoOpDataStore.memoize returns the output packet unchanged."""
    store = NoOpDataStore()

    # Create sample packets
    packet = {"input": "input_file.txt"}
    output_packet = {"output": "output_file.txt"}

    # Test memoize method
    result = store.memoize("test_store", "hash123", packet, output_packet)

    # NoOpDataStore should just return the output packet as is
    assert result == output_packet

    # Test with overwrite parameter
    result_with_overwrite = store.memoize(
        "test_store", "hash123", packet, output_packet, overwrite=True
    )
    assert result_with_overwrite == output_packet


def test_noop_data_store_retrieve_memoized():
    """Test that NoOpDataStore.retrieve_memoized always returns None."""
    store = NoOpDataStore()

    # Create sample packet
    packet = {"input": "input_file.txt"}

    # Test retrieve_memoized method
    result = store.retrieve_memoized("test_store", "hash123", packet)

    # NoOpDataStore should always return None for retrieve_memoized
    assert result is None


def test_noop_data_store_is_data_store_subclass():
    """Test that NoOpDataStore is a subclass of DataStore."""
    from orcapod.databases import DataStore

    store = NoOpDataStore()
    assert isinstance(store, DataStore)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
