"""Tests for FirstMatch mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import FirstMatch
from orcabridge.stream import SyncStreamFromLists


class TestFirstMatch:
    """Test cases for FirstMatch mapper."""

    def test_first_match_basic(self, simple_predicate):
        """Test basic first match functionality."""
        packets = [1, 2, 3, 4, 5]
        tags = ["odd", "even", "odd", "even", "odd"]

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(simple_predicate)
        result_stream = first_match(stream)

        result = list(result_stream)

        # Should find the first packet that matches the predicate
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == 2  # First even number
        assert tag == "even"

    def test_first_match_no_match(self, sample_packets, sample_tags):
        """Test first match when no packet matches."""

        def never_matches(packet, tag):
            return False

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        first_match = FirstMatch(never_matches)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 0

    def test_first_match_all_match(self, sample_packets, sample_tags):
        """Test first match when all packets match."""

        def always_matches(packet, tag):
            return True

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        first_match = FirstMatch(always_matches)
        result_stream = first_match(stream)

        result = list(result_stream)

        # Should return only the first packet
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == sample_packets[0]
        assert tag == sample_tags[0]

    def test_first_match_empty_stream(self, simple_predicate):
        """Test first match with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        first_match = FirstMatch(simple_predicate)
        result_stream = first_match(empty_stream)

        result = list(result_stream)
        assert len(result) == 0

    def test_first_match_string_predicate(self):
        """Test first match with string-based predicate."""
        packets = ["apple", "banana", "cherry", "date"]
        tags = ["fruit1", "fruit2", "fruit3", "fruit4"]

        def starts_with_c(packet, tag):
            return isinstance(packet, str) and packet.startswith("c")

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(starts_with_c)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == "cherry"
        assert tag == "fruit3"

    def test_first_match_tag_based_predicate(self):
        """Test first match using tag information."""
        packets = [10, 20, 30, 40]
        tags = ["small", "medium", "large", "huge"]

        def tag_contains_e(packet, tag):
            return "e" in tag

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(tag_contains_e)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == 20  # "medium" contains 'e'
        assert tag == "medium"

    def test_first_match_complex_predicate(self):
        """Test first match with complex predicate."""
        packets = [
            {"value": 5, "type": "A"},
            {"value": 15, "type": "B"},
            {"value": 25, "type": "A"},
            {"value": 35, "type": "C"},
        ]
        tags = ["item1", "item2", "item3", "item4"]

        def complex_predicate(packet, tag):
            return (
                isinstance(packet, dict)
                and packet.get("value", 0) > 10
                and packet.get("type") == "A"
            )

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(complex_predicate)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == {"value": 25, "type": "A"}
        assert tag == "item3"

    def test_first_match_with_none_packets(self):
        """Test first match with None packets."""
        packets = [None, "data", None, "more_data"]
        tags = ["empty1", "full1", "empty2", "full2"]

        def not_none(packet, tag):
            return packet is not None

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(not_none)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == "data"
        assert tag == "full1"

    def test_first_match_preserves_packet_types(self):
        """Test that first match preserves packet types."""
        packets = [PacketType("data1"), [1, 2, 3], {"key": "value"}, 42]
        tags = ["str", "list", "dict", "int"]

        def is_list(packet, tag):
            return isinstance(packet, list)

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(is_list)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == [1, 2, 3]
        assert tag == "list"
        assert isinstance(packet, list)

    def test_first_match_predicate_exception(self, sample_packets, sample_tags):
        """Test first match when predicate raises exception."""

        def error_predicate(packet, tag):
            if packet == sample_packets[1]:  # Error on second packet
                raise ValueError("Predicate error")
            return packet == sample_packets[2]  # Match third packet

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        first_match = FirstMatch(error_predicate)
        result_stream = first_match(stream)

        # The behavior here depends on implementation
        # It might propagate the exception or skip the problematic packet
        with pytest.raises(ValueError):
            list(result_stream)

    def test_first_match_with_generator_stream(self):
        """Test first match with generator-based stream."""

        def packet_generator():
            for i in range(10):
                yield f"packet_{i}", f"tag_{i}"

        from orcabridge.stream import SyncStreamFromGenerator

        stream = SyncStreamFromGenerator(packet_generator())

        def find_packet_5(packet, tag):
            return packet == "packet_5"

        first_match = FirstMatch(find_packet_5)
        result_stream = first_match(stream)

        result = list(result_stream)
        assert len(result) == 1
        packet, tag = result[0]
        assert packet == "packet_5"
        assert tag == "tag_5"

    def test_first_match_early_termination(self):
        """Test that first match terminates early and doesn't process remaining packets."""
        processed_packets = []

        def tracking_predicate(packet, tag):
            processed_packets.append(packet)
            return packet == "target"

        packets = ["a", "b", "target", "c", "d"]
        tags = ["tag1", "tag2", "tag3", "tag4", "tag5"]

        stream = SyncStreamFromLists(packets, tags)
        first_match = FirstMatch(tracking_predicate)
        result_stream = first_match(stream)

        result = list(result_stream)

        # Should have found the target
        assert len(result) == 1
        assert result[0][0] == "target"

        # Should have stopped processing after finding the target
        assert processed_packets == ["a", "b", "target"]

    def test_first_match_pickle(self):
        """Test that FirstMatch mapper is pickleable."""
        import pickle
        from orcabridge.mappers import FirstMatch

        first_match = FirstMatch()
        pickled = pickle.dumps(first_match)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, FirstMatch)
        assert unpickled.__class__.__name__ == "FirstMatch"
