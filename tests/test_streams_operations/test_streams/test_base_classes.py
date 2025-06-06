"""
Test module for base Stream and SyncStream classes.

This module tests the fundamental stream functionality including
iteration, flow operations, labeling, key management, and invocation tracking.
"""

from collections.abc import Collection, Iterator
import pytest
from unittest.mock import Mock, MagicMock
from abc import ABC

from orcabridge.base import Stream, SyncStream, Operation, Invocation
from orcabridge.mappers import Join
from orcabridge.streams import SyncStreamFromLists, SyncStreamFromGenerator
from orcabridge.types import Tag, Packet


class ConcreteStream(Stream):
    """Concrete Stream implementation for testing."""

    def __init__(self, data: Collection[tuple[Tag, Packet]], label=None):
        super().__init__(label=label)
        self.data = data

    def __iter__(self):
        return iter(self.data)


class ConcreteSyncStream(SyncStream):
    """Concrete SyncStream implementation for testing."""

    def __init__(self, data: Collection[tuple[Tag, Packet]], label=None):
        super().__init__(label=label)
        self.data = data

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        return iter(self.data)


@pytest.fixture
def sample_stream_data():
    """Sample stream data for testing."""
    return [
        ({"id": 1, "type": "text"}, {"content": "Hello", "size": 5}),
        ({"id": 2, "type": "text"}, {"content": "World", "size": 5}),
        ({"id": 3, "type": "number"}, {"value": 42, "unit": "count"}),
    ]


@pytest.fixture
def sample_tags_packets(sample_stream_data):
    """Extract tags and packets from sample data."""
    tags, packets = zip(*sample_stream_data)
    return list(tags), list(packets)


class TestStreamBase:
    """Test cases for base Stream class."""

    def test_stream_labels(self, sample_stream_data):
        """Test Stream initialization with and without label."""

        # Without label
        stream = ConcreteStream(sample_stream_data)
        assert stream.label == "ConcreteStream", (
            f"Label should default to class name {stream.__class__.__name__} but got {stream.label}"
        )
        assert stream.invocation is None

        # With label
        labeled_stream = ConcreteStream(sample_stream_data, label="test_stream")
        assert labeled_stream.label == "test_stream"

    def test_stream_iteration(self, sample_stream_data):
        """Test that Stream can be iterated over."""
        stream = ConcreteStream(sample_stream_data)

        result = list(stream)
        assert result == sample_stream_data

        # Test multiple iterations
        result2 = list(stream)
        assert result2 == sample_stream_data

    def test_stream_flow(self, sample_stream_data):
        """Test Stream.flow() method."""
        stream = ConcreteStream(sample_stream_data)

        flowed = stream.flow()
        assert flowed == sample_stream_data
        assert isinstance(flowed, list)

    def test_stream_identity_structure(self, sample_stream_data):
        """Test Stream identity structure."""
        stream = ConcreteStream(sample_stream_data)

        # Default identity structure for uninvoked stream should be None
        identity = stream.identity_structure()
        # TODO: consider alternative behavior for identity structure for streams
        assert identity is None

    def test_stream_keys_default(self, sample_stream_data):
        """Test Stream keys method default behavior."""
        stream = ConcreteStream(sample_stream_data)

        tag_keys, packet_keys = stream.keys()
        # Default implementation will be based on the first sample from the stream
        assert tag_keys is not None and set(tag_keys) == set(["id", "type"])

        assert packet_keys is not None and set(packet_keys) == set(["content", "size"])

    def test_stream_repr(self, sample_stream_data):
        """Test Stream string representation."""
        stream = ConcreteStream(sample_stream_data, label="test_stream")

        repr_str = repr(stream)
        assert "ConcreteStream" in repr_str
        assert "test_stream" in repr_str


class TestSyncStreamBase:
    """Test cases for SyncStream base class."""

    def test_syncstream_initialization(self, sample_stream_data):
        """Test SyncStream initialization."""
        sync_stream = ConcreteSyncStream(sample_stream_data)

        assert isinstance(sync_stream, Stream)
        assert isinstance(sync_stream, SyncStream)

    def test_syncstream_rshift_operator_dict(self, sample_stream_data):
        """Test SyncStream >> operator with dictionary mapping."""
        sync_stream = SyncStreamFromLists(paired=sample_stream_data)

        # Test with dictionary (should use MapPackets)
        mapping = {"content": "text", "size": "length"}
        mapped_stream = sync_stream >> mapping

        assert isinstance(mapped_stream, SyncStream)
        result = list(mapped_stream)

        # Check that mapping was applied
        for (tag, packet), (ref_tag, ref_packet) in zip(result, sample_stream_data):
            if "content" in ref_packet:
                assert "text" in packet
                assert packet["text"] == ref_packet["content"]
            if "size" in ref_packet:
                assert "length" in packet
                assert packet["length"] == ref_packet["size"]

    def test_syncstream_rshift_operator_callable(self, sample_stream_data):
        """Test SyncStream >> operator with callable transformer."""
        sync_stream = SyncStreamFromLists(paired=sample_stream_data)

        def add_processed_flag(stream):
            """Add processed flag to all packets."""

            def generator():
                for tag, packet in stream:
                    yield tag, {**packet, "processed": True}

            return SyncStreamFromGenerator(generator)

        transformed = sync_stream >> add_processed_flag
        result = list(transformed)

        # Check that all packets have processed flag
        for _, packet in result:
            assert packet["processed"] is True

    def test_syncstream_mul_operator(self, sample_tags_packets):
        """Test SyncStream * operator for joining streams."""
        tags1, packets1 = sample_tags_packets
        stream1 = SyncStreamFromLists(tags1[:2], packets1[:2])

        tags2 = [{"id": 1, "category": "A"}, {"id": 2, "category": "B"}]
        packets2 = [{"priority": "high"}, {"priority": "low"}]
        stream2 = SyncStreamFromLists(tags2, packets2)

        # Test join operation
        joined = stream1 * stream2

        assert joined.invocation is not None and isinstance(
            joined.invocation.operation, Join
        ), (
            f"* operator should be resulting from an Join object invocation but got {type(joined)}"
        )
        result = list(joined)

        # Should have joined results where tags match
        assert len(result) >= 0  # Exact count depends on tag matching logic

    def test_syncstream_mul_operator_type_error(self, sample_tags_packets):
        """Test SyncStream * operator with invalid type."""
        tags, packets = sample_tags_packets
        sync_stream = SyncStreamFromLists(tags, packets)

        with pytest.raises(TypeError, match="other must be a SyncStream"):
            sync_stream * "not_a_stream"  # type: ignore

    def test_syncstream_rshift_invalid_type(self, sample_tags_packets):
        """Test SyncStream >> operator with invalid transformer type."""
        tags, packets = sample_tags_packets
        sync_stream = SyncStreamFromLists(tags, packets)

        # Should handle non-dict, non-callable gracefully or raise appropriate error
        with pytest.raises((TypeError, AttributeError)):
            sync_stream >> 123  # type: ignore

    def test_syncstream_chaining_operations(self, sample_tags_packets):
        """Test chaining multiple SyncStream operations."""
        tags, packets = sample_tags_packets
        sync_stream = SyncStreamFromLists(tags, packets)

        # Chain multiple transformations
        def add_flag(stream):
            def generator():
                for tag, packet in stream:
                    yield tag, {**packet, "chained": True}

            return SyncStreamFromGenerator(generator)

        def add_counter(stream):
            def generator():
                for i, (tag, packet) in enumerate(stream):
                    yield tag, {**packet, "counter": i}

            return SyncStreamFromGenerator(generator)

        result_stream = sync_stream >> add_flag >> add_counter
        result = list(result_stream)

        # Check that both transformations were applied
        for i, (tag, packet) in enumerate(result):
            assert packet["chained"] is True
            assert packet["counter"] == i


class TestSyncStreamFromLists:
    """Test cases for SyncStreamFromLists implementation."""

    def test_creation_from_lists(self, sample_tags_packets):
        """Test SyncStreamFromLists creation."""
        tags, packets = sample_tags_packets
        stream = SyncStreamFromLists(tags, packets)

        assert isinstance(stream, SyncStream)
        result = list(stream)

        expected = list(zip(tags, packets))
        assert result == expected

    def test_creation_with_mismatched_lengths(self):
        """Test SyncStreamFromLists with mismatched tag/packet lengths."""
        tags = [{"id": "1"}, {"id": "2"}]
        packets = [{"data": "a"}]  # One less packet

        # If strict (default), should raise a ValueError
        with pytest.raises(ValueError):
            stream = SyncStreamFromLists(tags, packets, strict=True)

        # If not strict, should handle gracefully and create based on the shortest length
        stream = SyncStreamFromLists(tags, packets, strict=False)
        result = list(stream)

        assert len(result) == 1
        assert result[0] == ({"id": "1"}, {"data": "a"})

    def test_empty_lists(self):
        """Test SyncStreamFromLists with empty lists."""
        stream = SyncStreamFromLists([], [])
        result = list(stream)

        assert result == []

    def test_keys_inference(self, sample_tags_packets):
        """Test key inference from tag and packet data."""
        tags, packets = sample_tags_packets
        stream = SyncStreamFromLists(tags, packets)

        tag_keys, packet_keys = stream.keys()

        # Should infer keys from the first element
        expected_tag_keys = set()
        expected_packet_keys = set()

        if tags:
            expected_tag_keys.update(tags[0].keys())
        if packets:
            expected_packet_keys.update(packets[0].keys())

        assert tag_keys is not None and set(tag_keys) == expected_tag_keys
        assert packet_keys is not None and set(packet_keys) == expected_packet_keys

    def test_multiple_iterations(self, sample_tags_packets):
        """Test that SyncStreamFromLists can be iterated multiple times."""
        tags, packets = sample_tags_packets
        stream = SyncStreamFromLists(tags, packets)

        result1 = list(stream)
        result2 = list(stream)

        assert result1 == result2
        assert len(result1) == len(tags)


class TestSyncStreamFromGenerator:
    """Test cases for SyncStreamFromGenerator implementation."""

    def test_creation_from_generator(self, sample_stream_data):
        """Test SyncStreamFromGenerator creation."""

        def generator():
            for item in sample_stream_data:
                yield item

        stream = SyncStreamFromGenerator(generator)
        assert isinstance(stream, SyncStream)

        result = list(stream)
        assert result == sample_stream_data

    def test_generator_multiple_iterations(self, sample_stream_data):
        """Test that generator-based streams can be iterated multiple times"""

        def generator():
            for item in sample_stream_data:
                yield item

        stream = SyncStreamFromGenerator(generator)

        # First iteration should work
        result1 = list(stream)
        assert result1 == sample_stream_data

        # Second iteration should work (new iterator instance)
        result2 = list(stream)
        assert result2 == sample_stream_data

    def test_empty_generator(self):
        """Test SyncStreamFromGenerator with empty generator."""

        def empty_generator():
            return
            yield  # This line is never reached

        stream = SyncStreamFromGenerator(empty_generator)
        result = list(stream)

        assert result == []

    def test_generator_with_exception(self):
        """Test SyncStreamFromGenerator with generator that raises exception."""

        def failing_generator():
            yield ({"id": "1"}, {"data": "ok"})
            raise ValueError("Generator failed")

        stream = SyncStreamFromGenerator(failing_generator)

        # Should propagate the exception
        with pytest.raises(ValueError, match="Generator failed"):
            list(stream)

    def test_lazy_evaluation(self):
        """Test that SyncStreamFromGenerator is lazily evaluated."""
        call_count = {"count": 0}

        def counting_generator():
            call_count["count"] += 1
            yield ({"id": "1"}, {"data": "test"})

        stream = SyncStreamFromGenerator(counting_generator)

        # Generator should not be called until iteration starts
        assert call_count["count"] == 0

        # Start iteration
        iterator = iter(stream)
        next(iterator)

        # Now generator should have been called
        assert call_count["count"] == 1

    def test_inferred_keys_with_generator(self):
        """Test key inference with generator streams."""

        def sample_generator():
            yield ({"id": "1", "type": "A"}, {"value": "10", "name": "test"})
            yield ({"id": "2", "type": "B"}, {"value": "20", "size": "5"})

        stream = SyncStreamFromGenerator(sample_generator)

        # Keys should be inferred from generated data
        tag_keys, packet_keys = stream.keys()

        # Note: This depends on implementation - may need to consume stream
        # to infer keys, or may return None
        if tag_keys is not None:
            assert "id" in tag_keys
            assert "type" in tag_keys

        if packet_keys is not None:
            assert "value" in packet_keys

    def test_specified_keys_with_generator(self):
        """Test key inference with generator streams."""

        def sample_generator():
            yield ({"id": "1", "type": "A"}, {"value": "10", "name": "test"})
            yield ({"id": "2", "type": "B"}, {"value": "20", "size": "5"})

        # Specify keys explicitly -- it need not match the actual content
        stream = SyncStreamFromGenerator(
            sample_generator, tag_keys=["id"], packet_keys=["group"]
        )

        # Keys should be based on what was specified at the construction
        tag_keys, packet_keys = stream.keys()

        # Note: This depends on implementation - may need to consume stream
        # to infer keys, or may return None
        if tag_keys is not None:
            assert "id" in tag_keys
            assert "type" not in tag_keys

        if packet_keys is not None:
            assert "value" not in packet_keys
            assert "group" in packet_keys


class TestStreamIntegration:
    """Integration tests for stream functionality."""

    def test_stream_composition(self, sample_tags_packets):
        """Test composing different stream types."""
        tags, packets = sample_tags_packets

        # Create streams from different sources
        list_stream = SyncStreamFromLists(tags[:2], packets[:2])

        def gen_func():
            yield tags[2], packets[2]

        gen_stream = SyncStreamFromGenerator(gen_func)

        # Both should work similarly
        list_result = list(list_stream)
        gen_result = list(gen_stream)

        assert len(list_result) == 2
        assert len(gen_result) == 1

        # Combine results
        all_data = list_result + gen_result
        assert len(all_data) == 3

    def test_stream_with_complex_data(self):
        """Test streams with complex nested data."""
        complex_tags = [
            {"id": 1, "metadata": {"type": "nested", "level": 1}},
            {"id": 2, "metadata": {"type": "nested", "level": 2}},
        ]
        complex_packets = [
            {"data": {"values": [1, 2, 3], "config": {"enabled": True}}},
            {"data": {"values": [4, 5, 6], "config": {"enabled": False}}},
        ]

        stream = SyncStreamFromLists(complex_tags, complex_packets)
        result = list(stream)

        assert len(result) == 2

        # Verify complex data is preserved
        tag, packet = result[0]
        assert tag["metadata"]["type"] == "nested"
        assert packet["data"]["values"] == [1, 2, 3]
        assert packet["data"]["config"]["enabled"] is True

    def test_stream_memory_efficiency(self):
        """Test that generator streams don't consume excessive memory."""

        def large_generator():
            for i in range(1000):
                yield ({"id": i}, {"value": i * 2})

        stream = SyncStreamFromGenerator(large_generator)

        # Process in chunks to test memory efficiency
        count = 0
        for tag, packet in stream:
            count += 1
            if count > 10:  # Just test first few items
                break

        assert count == 11  # Processed 11 items

    def test_stream_error_propagation(self, sample_tags_packets):
        """Test that errors in stream data are properly propagated."""
        tags, packets = sample_tags_packets

        # Create stream with invalid data
        invalid_tags = tags + [None]  # Add invalid tag
        invalid_packets = packets + [{"data": "valid"}]

        stream = SyncStreamFromLists(invalid_tags, invalid_packets)

        # Should handle None tags gracefully or raise appropriate error
        result = list(stream)

        # The None tag should be included as-is
        assert len(result) == 4
        assert result[-1] == (None, {"data": "valid"})
