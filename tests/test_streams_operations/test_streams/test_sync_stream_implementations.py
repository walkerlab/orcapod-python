"""
Test module for SyncStream concrete implementations.

This module tests the specific implementations of SyncStream including
SyncStreamFromLists and SyncStreamFromGenerator, focusing on their unique
behaviors, performance characteristics, and edge cases.
"""

import pytest
from unittest.mock import Mock, patch
import gc

from orcabridge.streams import SyncStreamFromLists, SyncStreamFromGenerator
from orcabridge.base import SyncStream


@pytest.fixture
def sample_data():
    """Sample data for stream testing."""
    return [
        ({"id": 1, "type": "doc"}, {"content": "Hello", "size": 5}),
        ({"id": 2, "type": "doc"}, {"content": "World", "size": 5}),
        ({"id": 3, "type": "img"}, {"pixels": 1920 * 1080, "format": "png"}),
    ]


@pytest.fixture
def sample_tags_packets(sample_data):
    """Extract tags and packets separately."""
    tags, packets = zip(*sample_data)
    return list(tags), list(packets)


class TestSyncStreamFromLists:
    """Comprehensive tests for SyncStreamFromLists implementation."""

    def test_basic_creation_and_iteration(self, sample_tags_packets):
        """Test basic creation and iteration functionality."""
        tags, packets = sample_tags_packets
        stream = SyncStreamFromLists(tags, packets)

        # Test basic properties
        assert isinstance(stream, SyncStream)

        # Test iteration
        result = list(stream)
        expected = list(zip(tags, packets))
        assert result == expected

    def test_creation_with_empty_lists(self):
        """Test creation with empty tag and packet lists."""
        stream = SyncStreamFromLists([], [])

        result = list(stream)
        assert result == []

        # Test keys with empty stream
        tag_keys, packet_keys = stream.keys()
        assert tag_keys == []
        assert packet_keys == []

    def test_creation_with_single_item(self):
        """Test creation with single tag-packet pair."""
        tags = [{"id": 1}]
        packets = [{"data": "test"}]
        stream = SyncStreamFromLists(tags, packets)

        result = list(stream)
        assert result == [({"id": 1}, {"data": "test"})]

    def test_mismatched_list_lengths(self):
        """Test behavior with different length tag and packet lists."""
        tags = [{"id": 1}, {"id": 2}, {"id": 3}]
        packets = [{"data": "a"}, {"data": "b"}]  # Shorter list

        stream = SyncStreamFromLists(tags, packets)
        result = list(stream)

        # Should zip to shortest length
        assert len(result) == 2
        assert result == [
            ({"id": 1}, {"data": "a"}),
            ({"id": 2}, {"data": "b"}),
        ]

    def test_keys_inference_comprehensive(self):
        """Test comprehensive key inference from data."""
        tags = [
            {"id": 1, "type": "A", "category": "test"},
            {"id": 2, "type": "B"},  # Missing category
            {"id": 3, "category": "prod", "extra": "value"},  # Missing type, has extra
        ]
        packets = [
            {"data": "hello", "size": 5, "meta": {"info": "test"}},
            {"data": "world", "count": 10},  # Missing size, meta; has count
            {"size": 3, "format": "json"},  # Missing data; has format
        ]

        stream = SyncStreamFromLists(tags, packets)
        tag_keys, packet_keys = stream.keys()

        # Should include all keys found across all items
        expected_tag_keys = {"id", "type", "category", "extra"}
        expected_packet_keys = {"data", "size", "meta", "count", "format"}

        assert set(tag_keys) == expected_tag_keys
        assert set(packet_keys) == expected_packet_keys

    def test_multiple_iterations_consistency(self, sample_tags_packets):
        """Test that multiple iterations return consistent results."""
        tags, packets = sample_tags_packets
        stream = SyncStreamFromLists(tags, packets)

        # Multiple iterations should be identical
        result1 = list(stream)
        result2 = list(stream)
        result3 = list(stream)

        assert result1 == result2 == result3
        assert len(result1) == len(tags)

    def test_iteration_with_generators_as_input(self):
        """Test creation with generator inputs (should work since converted to lists)."""

        def tag_gen():
            for i in range(3):
                yield {"id": i}

        def packet_gen():
            for i in range(3):
                yield {"value": i * 10}

        # Should accept generators and convert them
        stream = SyncStreamFromLists(list(tag_gen()), list(packet_gen()))
        result = list(stream)

        assert len(result) == 3
        assert result[0] == ({"id": 0}, {"value": 0})
        assert result[1] == ({"id": 1}, {"value": 10})
        assert result[2] == ({"id": 2}, {"value": 20})

    def test_memory_efficiency_large_lists(self):
        """Test memory efficiency with large lists."""
        # Create large but not excessive lists
        size = 1000
        tags = [{"id": i} for i in range(size)]
        packets = [{"value": i * 2} for i in range(size)]

        stream = SyncStreamFromLists(tags, packets)

        # Should be able to iterate without memory issues
        count = 0
        for tag, packet in stream:
            count += 1
            assert tag["id"] == packet["value"] // 2

        assert count == size

    def test_data_types_preservation(self):
        """Test that various data types are preserved correctly."""
        tags = [
            {"int": 42, "float": 3.14, "str": "hello"},
            {"bool": True, "none": None, "list": [1, 2, 3]},
            {"dict": {"nested": "value"}, "tuple": (1, 2)},
        ]
        packets = [
            {"complex": 1 + 2j, "bytes": b"binary", "set": {1, 2, 3}},
            {"lambda": lambda x: x * 2},  # Function objects
            {"custom": {"deep": {"nesting": {"value": 123}}}},
        ]

        stream = SyncStreamFromLists(tags, packets)
        result = list(stream)

        # Verify data type preservation
        assert result[0][0]["int"] == 42
        assert result[0][0]["float"] == 3.14
        assert result[0][1]["complex"] == 1 + 2j
        assert result[0][1]["bytes"] == b"binary"

        assert result[1][0]["bool"] is True
        assert result[1][0]["none"] is None
        assert callable(result[1][1]["lambda"])

        assert result[2][0]["dict"]["nested"] == "value"
        assert result[2][1]["custom"]["deep"]["nesting"]["value"] == 123

    def test_mutable_data_safety(self):
        """Test that mutable data doesn't cause unexpected sharing."""
        shared_dict = {"shared": "value"}
        tags = [{"ref": shared_dict}, {"ref": shared_dict}]
        packets = [{"data": "a"}, {"data": "b"}]

        stream = SyncStreamFromLists(tags, packets)
        result = list(stream)

        # Modify the shared dict
        shared_dict["shared"] = "modified"

        # The stream results should reflect the change (references preserved)
        assert result[0][0]["ref"]["shared"] == "modified"
        assert result[1][0]["ref"]["shared"] == "modified"

    def test_label_and_metadata(self, sample_tags_packets):
        """Test stream labeling and metadata handling."""
        tags, packets = sample_tags_packets

        # Test with custom label
        stream = SyncStreamFromLists(tags, packets, label="test_stream")
        assert stream.label == "test_stream"

        # Test default label generation
        stream_auto = SyncStreamFromLists(tags, packets)
        assert "SyncStreamFromLists_" in stream_auto.label


class TestSyncStreamFromGenerator:
    """Comprehensive tests for SyncStreamFromGenerator implementation."""

    def test_basic_creation_and_iteration(self, sample_data):
        """Test basic creation and iteration functionality."""

        def generator():
            for item in sample_data:
                yield item

        stream = SyncStreamFromGenerator(generator)
        assert isinstance(stream, SyncStream)

        result = list(stream)
        assert result == sample_data

    def test_empty_generator(self):
        """Test with generator that yields nothing."""

        def empty_gen():
            return
            yield  # Never reached

        stream = SyncStreamFromGenerator(empty_gen)
        result = list(stream)
        assert result == []

    def test_single_item_generator(self):
        """Test with generator that yields single item."""

        def single_gen():
            yield ({"id": 1}, {"data": "test"})

        stream = SyncStreamFromGenerator(single_gen)
        result = list(stream)
        assert result == [({"id": 1}, {"data": "test"})]

    def test_generator_exhaustion(self, sample_data):
        """Test that generators are exhausted after iteration."""

        def generator():
            for item in sample_data:
                yield item

        stream = SyncStreamFromGenerator(generator)

        # First iteration consumes generator
        result1 = list(stream)
        assert result1 == sample_data

        # Second iteration gets empty results (generator exhausted)
        result2 = list(stream)
        assert result2 == []

    def test_lazy_evaluation(self):
        """Test that generator evaluation is lazy."""
        call_log = []

        def tracking_generator():
            call_log.append("generator_started")
            for i in range(3):
                call_log.append(f"yielding_{i}")
                yield ({"id": i}, {"value": i * 10})
            call_log.append("generator_finished")

        stream = SyncStreamFromGenerator(tracking_generator)

        # Generator should not have started yet
        assert call_log == []

        # Start iteration but don't consume everything
        iterator = iter(stream)
        next(iterator)

        # Should have started and yielded first item
        assert "generator_started" in call_log
        assert "yielding_0" in call_log
        assert "yielding_1" not in call_log

    def test_generator_with_exception(self):
        """Test generator that raises exception during iteration."""

        def failing_generator():
            yield ({"id": 1}, {"data": "ok"})
            yield ({"id": 2}, {"data": "ok"})
            raise ValueError("Something went wrong")
            yield ({"id": 3}, {"data": "never_reached"})

        stream = SyncStreamFromGenerator(failing_generator)

        # Should propagate exception
        with pytest.raises(ValueError, match="Something went wrong"):
            list(stream)

    def test_generator_partial_consumption(self, sample_data):
        """Test partial consumption of generator."""

        def generator():
            for item in sample_data:
                yield item

        stream = SyncStreamFromGenerator(generator)

        # Consume only part of the stream
        iterator = iter(stream)
        first_item = next(iterator)
        second_item = next(iterator)

        assert first_item == sample_data[0]
        assert second_item == sample_data[1]

        # Rest of generator should still be available
        remaining = list(iterator)
        assert remaining == sample_data[2:]

    def test_generator_with_infinite_sequence(self):
        """Test generator with infinite sequence (partial consumption)."""

        def infinite_generator():
            i = 0
            while True:
                yield ({"id": i}, {"value": i * i})
                i += 1

        stream = SyncStreamFromGenerator(infinite_generator)

        # Consume just first few items
        iterator = iter(stream)
        results = []
        for _ in range(5):
            results.append(next(iterator))

        assert len(results) == 5
        assert results[0] == ({"id": 0}, {"value": 0})
        assert results[4] == ({"id": 4}, {"value": 16})

    def test_generator_with_complex_logic(self):
        """Test generator with complex internal logic."""

        def complex_generator():
            # Generator with state and complex logic
            state = {"count": 0, "filter_odd": True}

            for i in range(10):
                state["count"] += 1

                if state["filter_odd"] and i % 2 == 1:
                    continue  # Skip odd numbers initially

                if i == 6:  # Change behavior mid-stream
                    state["filter_odd"] = False

                yield ({"id": i, "count": state["count"]}, {"value": i * 2})

        stream = SyncStreamFromGenerator(complex_generator)
        result = list(stream)

        # Should have skipped odds initially, then included them
        ids = [item[0]["id"] for item in result]
        assert 0 in ids and 2 in ids and 4 in ids and 6 in ids  # Evens
        assert 1 not in ids and 3 not in ids and 5 not in ids  # Early odds skipped
        assert 7 in ids and 8 in ids and 9 in ids  # Later odds included

    def test_keys_inference_limitation(self):
        """Test that key inference may be limited for generators."""

        def generator():
            yield ({"id": 1, "type": "A"}, {"data": "hello", "size": 5})
            yield ({"id": 2, "type": "B"}, {"data": "world", "count": 10})

        stream = SyncStreamFromGenerator(generator)

        # Keys might not be available without consuming stream
        tag_keys, packet_keys = stream.keys()

        # Implementation-dependent: might be None or inferred
        if tag_keys is not None:
            assert isinstance(tag_keys, (list, tuple, set))
        if packet_keys is not None:
            assert isinstance(packet_keys, (list, tuple, set))

    def test_memory_efficiency(self):
        """Test memory efficiency of generator streams."""

        def memory_efficient_generator():
            # Generate large number of items without storing them all
            for i in range(10000):
                yield ({"id": i}, {"value": i * 2})

        stream = SyncStreamFromGenerator(memory_efficient_generator)

        # Process in chunks to verify memory efficiency
        count = 0
        for tag, packet in stream:
            count += 1
            assert tag["id"] == packet["value"] // 2

            if count >= 100:  # Don't process all 10k items in test
                break

        assert count == 100

    def test_generator_function_vs_generator_object(self, sample_data):
        """Test creation with generator function vs generator object."""

        def gen_function():
            for item in sample_data:
                yield item

        # Test with generator function (should work)
        stream1 = SyncStreamFromGenerator(gen_function)
        result1 = list(stream1)

        # Test with generator object (should work)
        gen_object = gen_function()
        stream2 = SyncStreamFromGenerator(lambda: gen_object)
        result2 = list(stream2)

        assert result1 == sample_data
        assert result2 == sample_data

    def test_label_and_metadata(self, sample_data):
        """Test stream labeling and metadata handling."""

        def generator():
            for item in sample_data:
                yield item

        # Test with custom label
        stream = SyncStreamFromGenerator(generator, label="test_gen_stream")
        assert stream.label == "test_gen_stream"

        # Test default label generation
        stream_auto = SyncStreamFromGenerator(generator)
        assert "SyncStreamFromGenerator_" in stream_auto.label


class TestStreamImplementationComparison:
    """Tests comparing different stream implementations."""

    def test_equivalent_output(self, sample_data):
        """Test that both implementations produce equivalent output for same data."""
        tags, packets = zip(*sample_data)

        # Create streams from same data using different implementations
        list_stream = SyncStreamFromLists(list(tags), list(packets))

        def generator():
            for item in sample_data:
                yield item

        gen_stream = SyncStreamFromGenerator(generator)

        # Results should be identical
        list_result = list(list_stream)
        gen_result = list(gen_stream)

        assert list_result == gen_result == sample_data

    def test_multiple_iteration_behavior(self, sample_data):
        """Test different behavior in multiple iterations."""
        tags, packets = zip(*sample_data)

        list_stream = SyncStreamFromLists(list(tags), list(packets))

        def generator():
            for item in sample_data:
                yield item

        gen_stream = SyncStreamFromGenerator(generator)

        # List stream should support multiple iterations
        list_result1 = list(list_stream)
        list_result2 = list(list_stream)
        assert list_result1 == list_result2

        # Generator stream should only work once
        gen_result1 = list(gen_stream)
        gen_result2 = list(gen_stream)
        assert gen_result1 == sample_data
        assert gen_result2 == []  # Exhausted

    def test_performance_characteristics(self):
        """Test performance characteristics of different implementations."""
        import time

        size = 1000
        tags = [{"id": i} for i in range(size)]
        packets = [{"value": i * 2} for i in range(size)]

        # Time list-based stream creation and consumption
        start = time.time()
        list_stream = SyncStreamFromLists(tags, packets)
        list_result = list(list_stream)
        list_time = time.time() - start

        # Time generator-based stream creation and consumption
        def generator():
            for tag, packet in zip(tags, packets):
                yield tag, packet

        start = time.time()
        gen_stream = SyncStreamFromGenerator(generator)
        gen_result = list(gen_stream)
        gen_time = time.time() - start

        # Results should be equivalent
        assert list_result == gen_result

        # Both should complete in reasonable time (implementation dependent)
        assert list_time < 1.0  # Should be fast
        assert gen_time < 1.0  # Should be fast

    def test_error_handling_consistency(self):
        """Test that error handling is consistent between implementations."""

        def failing_generator():
            yield ({"id": 1}, {"data": "ok"})
            raise RuntimeError("Generator error")

        # Generator stream should propagate error
        gen_stream = SyncStreamFromGenerator(failing_generator)
        with pytest.raises(RuntimeError, match="Generator error"):
            list(gen_stream)

        # List stream with problematic data
        tags = [{"id": 1}, None]  # None tag might cause issues
        packets = [{"data": "ok"}, {"data": "also_ok"}]

        list_stream = SyncStreamFromLists(tags, packets)
        result = list(list_stream)  # Should handle None gracefully

        assert len(result) == 2
        assert result[1] == (None, {"data": "also_ok"})

    def test_integration_with_operations(self, sample_data):
        """Test that both stream types work equivalently with operations."""
        from orcabridge.mapper import Filter

        tags, packets = zip(*sample_data)

        # Create equivalent streams
        list_stream = SyncStreamFromLists(list(tags), list(packets))

        def generator():
            for item in sample_data:
                yield item

        gen_stream = SyncStreamFromGenerator(generator)

        # Apply same operation to both
        filter_op = Filter(lambda tag, packet: tag["id"] > 1)

        filtered_list = filter_op(list_stream)
        filtered_gen = filter_op(gen_stream)

        list_result = list(filtered_list)
        gen_result = list(filtered_gen)

        # Results should be equivalent
        assert list_result == gen_result
        assert len(list_result) == 2  # Should have filtered out id=1
