"""Tests for Join mapper functionality."""

import pytest
import pickle
from orcabridge.mappers import Join
from orcabridge.streams import SyncStreamFromLists


class TestJoin:
    """Test cases for Join mapper."""

    def test_join_basic(self, sample_packets, sample_tags):
        """Test basic join functionality."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        join = Join()
        joined_stream = join(stream)

        # Join should collect all packets into a single packet
        packets = list(joined_stream)

        assert len(packets) == 1
        joined_packet, joined_tag = packets[0]

        # The joined packet should contain all original packets
        assert len(joined_packet) == len(sample_packets)
        assert list(joined_packet) == sample_packets

    def test_join_empty_stream(self):
        """Test join with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        join = Join()
        joined_stream = join(empty_stream)

        packets = list(joined_stream)

        assert len(packets) == 1
        joined_packet, _ = packets[0]
        assert len(joined_packet) == 0
        assert list(joined_packet) == []

    def test_join_single_packet(self):
        """Test join with single packet stream."""
        packets = ["single_packet"]
        tags = ["single_tag"]
        stream = SyncStreamFromLists(packets, tags)

        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        assert len(result) == 1

        joined_packet, joined_tag = result[0]
        assert len(joined_packet) == 1
        assert list(joined_packet) == ["single_packet"]

    def test_join_preserves_packet_types(self):
        """Test that join preserves different packet types."""
        packets = [PacketType("data1"), {"key": "value"}, [1, 2, 3], 42, "string"]
        tags = ["type1", "type2", "type3", "type4", "type5"]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        assert len(result) == 1

        joined_packet, _ = result[0]
        assert len(joined_packet) == 5

        joined_list = list(joined_packet)
        assert joined_list[0] == PacketType("data1")
        assert joined_list[1] == {"key": "value"}
        assert joined_list[2] == [1, 2, 3]
        assert joined_list[3] == 42
        assert joined_list[4] == "string"

    def test_join_maintains_order(self):
        """Test that join maintains packet order."""
        packets = [f"packet_{i}" for i in range(10)]
        tags = [f"tag_{i}" for i in range(10)]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        joined_packet, _ = result[0]

        assert list(joined_packet) == packets

    def test_join_tag_handling(self, sample_packets, sample_tags):
        """Test how join handles tags."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        _, joined_tag = result[0]

        # The joined tag should be a collection of original tags
        # (implementation-specific behavior)
        assert joined_tag is not None

    def test_join_large_stream(self):
        """Test join with large stream."""
        packets = [f"packet_{i}" for i in range(1000)]
        tags = [f"tag_{i}" for i in range(1000)]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        assert len(result) == 1

        joined_packet, _ = result[0]
        assert len(joined_packet) == 1000
        assert list(joined_packet) == packets

    def test_join_nested_structures(self):
        """Test join with nested data structures."""
        packets = [{"nested": {"data": 1}}, [1, [2, 3], 4], ((1, 2), (3, 4))]
        tags = ["dict", "list", "tuple"]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        joined_packet, _ = result[0]

        joined_list = list(joined_packet)
        assert joined_list[0] == {"nested": {"data": 1}}
        assert joined_list[1] == [1, [2, 3], 4]
        assert joined_list[2] == ((1, 2), (3, 4))

    def test_join_with_none_packets(self):
        """Test join with None packets."""
        packets = ["data1", None, "data2", None]
        tags = ["tag1", "tag2", "tag3", "tag4"]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        result = list(joined_stream)
        joined_packet, _ = result[0]

        joined_list = list(joined_packet)
        assert joined_list == ["data1", None, "data2", None]

    def test_join_chaining(self, sample_packets, sample_tags):
        """Test chaining join operations."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)

        # First join
        join1 = Join()
        joined_stream1 = join1(stream)

        # Second join (should join the already joined result)
        join2 = Join()
        joined_stream2 = join2(joined_stream1)

        result = list(joined_stream2)
        assert len(result) == 1

        # The result should be a packet containing one element (the previous join result)
        final_packet, _ = result[0]
        assert len(final_packet) == 1

    def test_join_memory_efficiency(self):
        """Test that join doesn't consume excessive memory for large streams."""
        # This is more of a performance test, but we can check basic functionality
        packets = [f"packet_{i}" for i in range(10000)]
        tags = [f"tag_{i}" for i in range(10000)]

        stream = SyncStreamFromLists(packets, tags)
        join = Join()
        joined_stream = join(stream)

        # Just verify it completes without issues
        result = list(joined_stream)
        assert len(result) == 1

        joined_packet, _ = result[0]
        assert len(joined_packet) == 10000

    def test_join_pickle(self):
        """Test that Join mapper is pickleable."""
        join = Join()
        pickled = pickle.dumps(join)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, Join)
        assert unpickled.__class__.__name__ == "Join"
