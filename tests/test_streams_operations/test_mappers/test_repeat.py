"""Tests for Repeat mapper functionality."""

import pytest
import pickle
from orcabridge.mappers import Repeat


class TestRepeat:
    """Test cases for Repeat mapper."""

    def test_repeat_basic(self, sample_stream):
        """Test basic repeat functionality."""
        repeat = Repeat(3)
        repeated_stream = repeat(sample_stream)

        packets = list(repeated_stream)

        # Should have 3 times the original packets
        assert len(packets) == 9  # 3 original * 3 repeats

        # Check that each packet appears 3 times consecutively
        original_packets = list(sample_stream)
        expected_packets = []
        for packet in original_packets:
            expected_packets.extend([packet] * 3)

        assert packets == expected_packets

    def test_repeat_zero(self, sample_stream):
        """Test repeat with count 0."""
        repeat = Repeat(0)
        repeated_stream = repeat(sample_stream)

        packets = list(repeated_stream)
        assert len(packets) == 0

    def test_repeat_one(self, sample_stream):
        """Test repeat with count 1."""
        repeat = Repeat(1)
        repeated_stream = repeat(sample_stream)

        packets = list(repeated_stream)
        original_packets = list(sample_stream)

        assert packets == original_packets

    def test_repeat_with_tags(self, sample_packets, sample_tags):
        """Test repeat preserves tags correctly."""
        from orcabridge.streams import SyncStreamFromLists

        stream = SyncStreamFromLists(tags=sample_tags, packets=sample_packets)
        repeat = Repeat(2)
        repeated_stream = repeat(stream)

        packets = []
        tags = []
        for tag, packet in repeated_stream:
            packets.append(packet)
            tags.append(tag)

        # Each packet should appear twice with its corresponding tag
        assert len(packets) == 6  # 3 original * 2 repeats
        assert len(tags) == 6

        # Check pattern: [p1,p1,p2,p2,p3,p3] with [t1,t1,t2,t2,t3,t3]
        expected_packets = []
        expected_tags = []
        for p, t in zip(sample_packets, sample_tags):
            expected_packets.extend([p, p])
            expected_tags.extend([t, t])

        assert packets == expected_packets
        assert tags == expected_tags

    def test_repeat_with_empty_stream(self):
        """Test repeat with empty stream."""
        from orcabridge.streams import SyncStreamFromLists

        empty_stream = SyncStreamFromLists(tags=[], packets=[])
        repeat = Repeat(5)
        repeated_stream = repeat(empty_stream)

        packets = list(repeated_stream)
        assert len(packets) == 0

    def test_repeat_large_count(self, sample_stream):
        """Test repeat with large count."""
        repeat = Repeat(100)
        repeated_stream = repeat(sample_stream)

        packets = list(repeated_stream)
        assert len(packets) == 300  # 3 original * 100 repeats

    def test_repeat_negative_count(self):
        """Test repeat with negative count raises error."""
        with pytest.raises(ValueError):
            Repeat(-1)

    def test_repeat_non_integer_count(self):
        """Test repeat with non-integer count."""
        with pytest.raises(TypeError):
            Repeat(3.5)

        with pytest.raises(TypeError):
            Repeat("3")

    def test_repeat_preserves_packet_types(self, sample_stream):
        """Test that repeat preserves different packet types."""
        # Create stream with mixed packet types
        from orcabridge.streams import SyncStreamFromLists

        packets = [
            {"data": "data1"},
            {"key": "value"},
            {"items": ["a", "b", "c"]},
            {"number": "42"},
        ]
        tags = [{"type": "str"}, {"type": "dict"}, {"type": "list"}, {"type": "int"}]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        repeat = Repeat(2)
        repeated_stream = repeat(stream)

        result_packets = []
        for tag, packet in repeated_stream:
            result_packets.append(packet)

        expected = [
            {"data": "data1"},
            {"data": "data1"},
            {"key": "value"},
            {"key": "value"},
            {"items": ["a", "b", "c"]},
            {"items": ["a", "b", "c"]},
            {"number": "42"},
            {"number": "42"},
        ]

        assert result_packets == expected

    def test_repeat_chaining(self, sample_stream):
        """Test chaining multiple repeat operations."""
        repeat1 = Repeat(2)
        repeat2 = Repeat(3)

        # Apply first repeat
        stream1 = repeat1(sample_stream)
        # Apply second repeat
        stream2 = repeat2(stream1)

        packets = list(stream2)

        # Should have 3 original * 2 * 3 = 18 packets
        assert len(packets) == 18

        # Each original packet should appear 6 times consecutively
        original_packets = list(sample_stream)
        expected = []
        for packet in original_packets:
            expected.extend([packet] * 6)

        assert packets == expected

    def test_repeat_pickle(self):
        """Test that Repeat mapper is pickleable."""
        repeat = Repeat(5)

        # Test pickle/unpickle
        pickled = pickle.dumps(repeat)
        unpickled = pickle.loads(pickled)

        # Verify the unpickled mapper has the same properties
        assert unpickled.repeat_count == repeat.repeat_count

        # Test that the unpickled mapper works correctly
        from orcabridge.streams import SyncStreamFromLists

        tags = [{"id": "1"}, {"id": "2"}]
        packets = [{"data": "file1.txt"}, {"data": "file2.txt"}]
        stream = SyncStreamFromLists(tags=tags, packets=packets)

        original_results = list(repeat(stream))
        unpickled_results = list(unpickled(stream))

        assert original_results == unpickled_results
        assert len(original_results) == 10  # 2 * 5 repeats
