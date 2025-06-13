"""Tests for Merge mapper functionality."""

import pickle
import pytest
from orcabridge.base import PacketType
from orcabridge.mappers import Merge
from orcabridge.streams import SyncStreamFromLists


class TestMerge:
    """Test cases for Merge mapper."""

    def test_merge_two_streams(self, sample_packets, sample_tags):
        """Test merging two streams."""
        # Create two streams
        stream1 = SyncStreamFromLists(sample_packets[:2], sample_tags[:2])
        stream2 = SyncStreamFromLists(sample_packets[2:], sample_tags[2:])

        merge = Merge()
        merged_stream = merge(stream1, stream2)

        packets = []
        tags = []
        for packet, tag in merged_stream:
            packets.append(packet)
            tags.append(tag)

        # Should contain all packets from both streams
        assert len(packets) == 3
        assert set(packets) == set(sample_packets)
        assert set(tags) == set(sample_tags)

    def test_merge_multiple_streams(self, sample_packets, sample_tags):
        """Test merging multiple streams."""
        # Create three streams with one packet each
        streams = []
        for i in range(3):
            stream = SyncStreamFromLists([sample_packets[i]], [sample_tags[i]])
            streams.append(stream)

        merge = Merge()
        merged_stream = merge(*streams)

        packets = []
        tags = []
        for packet, tag in merged_stream:
            packets.append(packet)
            tags.append(tag)

        assert len(packets) == 3
        assert set(packets) == set(sample_packets)
        assert set(tags) == set(sample_tags)

    def test_merge_empty_streams(self):
        """Test merging with empty streams."""
        empty1 = SyncStreamFromLists([], [])
        empty2 = SyncStreamFromLists([], [])

        merge = Merge()
        merged_stream = merge(empty1, empty2)

        packets = list(merged_stream)
        assert len(packets) == 0

    def test_merge_one_empty_one_full(self, sample_stream):
        """Test merging empty stream with full stream."""
        empty_stream = SyncStreamFromLists([], [])

        merge = Merge()
        merged_stream = merge(sample_stream, empty_stream)

        packets = list(merged_stream)
        original_packets = list(sample_stream)

        assert len(packets) == len(original_packets)
        # Order might be different, so check sets
        assert set(packets) == set(original_packets)

    def test_merge_different_lengths(self):
        """Test merging streams of different lengths."""
        packets1 = ["a", "b"]
        tags1 = ["tag1", "tag2"]
        packets2 = ["c", "d", "e", "f"]
        tags2 = ["tag3", "tag4", "tag5", "tag6"]

        stream1 = SyncStreamFromLists(packets1, tags1)
        stream2 = SyncStreamFromLists(packets2, tags2)

        merge = Merge()
        merged_stream = merge(stream1, stream2)

        packets = []
        tags = []
        for packet, tag in merged_stream:
            packets.append(packet)
            tags.append(tag)

        assert len(packets) == 6
        assert set(packets) == set(packets1 + packets2)
        assert set(tags) == set(tags1 + tags2)

    def test_merge_single_stream(self, sample_stream):
        """Test merge with single stream."""
        merge = Merge()
        merged_stream = merge(sample_stream)

        packets = list(merged_stream)
        original_packets = list(sample_stream)

        assert packets == original_packets

    def test_merge_preserves_packet_types(self):
        """Test that merge preserves different packet types."""
        packets1 = [PacketType("data1"), {"key1": "value1"}]
        tags1 = ["str1", "dict1"]
        packets2 = [[1, 2], 42]
        tags2 = ["list1", "int1"]

        stream1 = SyncStreamFromLists(packets1, tags1)
        stream2 = SyncStreamFromLists(packets2, tags2)

        merge = Merge()
        merged_stream = merge(stream1, stream2)

        result_packets = []
        for packet, _ in merged_stream:
            result_packets.append(packet)

        assert len(result_packets) == 4
        assert set(result_packets) == set(packets1 + packets2)

    def test_merge_order_independence(self, sample_packets, sample_tags):
        """Test that merge order doesn't affect final result set."""
        stream1 = SyncStreamFromLists(sample_packets[:2], sample_tags[:2])
        stream2 = SyncStreamFromLists(sample_packets[2:], sample_tags[2:])

        merge = Merge()

        # Merge in one order
        merged1 = merge(stream1, stream2)
        packets1 = set(p for p, _ in merged1)

        # Merge in reverse order (need to recreate streams)
        stream1_new = SyncStreamFromLists(sample_packets[:2], sample_tags[:2])
        stream2_new = SyncStreamFromLists(sample_packets[2:], sample_tags[2:])
        merged2 = merge(stream2_new, stream1_new)
        packets2 = set(p for p, _ in merged2)

        assert packets1 == packets2

    def test_merge_with_duplicate_packets(self):
        """Test merging streams with duplicate packets."""
        packets1 = ["a", "b"]
        tags1 = ["tag1", "tag2"]
        packets2 = ["a", "c"]  # "a" appears in both streams
        tags2 = ["tag3", "tag4"]

        stream1 = SyncStreamFromLists(packets1, tags1)
        stream2 = SyncStreamFromLists(packets2, tags2)

        merge = Merge()
        merged_stream = merge(stream1, stream2)

        packets = []
        for packet, _ in merged_stream:
            packets.append(packet)

        # Should include duplicates
        assert len(packets) == 4
        assert packets.count("a") == 2
        assert "b" in packets
        assert "c" in packets

    def test_merge_no_streams_error(self):
        """Test that merge with no streams raises an error."""
        merge = Merge()

        with pytest.raises(TypeError):
            merge()

    def test_merge_large_number_of_streams(self):
        """Test merging a large number of streams."""
        streams = []
        all_packets = []

        for i in range(10):
            packets = [f"packet_{i}"]
            tags = [f"tag_{i}"]
            streams.append(SyncStreamFromLists(packets, tags))
            all_packets.extend(packets)

        merge = Merge()
        merged_stream = merge(*streams)

        result_packets = []
        for packet, _ in merged_stream:
            result_packets.append(packet)

        assert len(result_packets) == 10
        assert set(result_packets) == set(all_packets)
        """Test that Merge mapper is pickleable."""
        merge = Merge()
        pickled = pickle.dumps(merge)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, Merge)
        assert unpickled.__class__.__name__ == "Merge"
