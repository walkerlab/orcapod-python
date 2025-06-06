"""Tests for Filter mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import Filter
from orcabridge.stream import SyncStreamFromLists


class TestFilter:
    """Test cases for Filter mapper."""

    def test_filter_basic(self, simple_predicate):
        """Test basic filter functionality."""
        packets = [1, 2, 3, 4, 5, 6]
        tags = ["odd", "even", "odd", "even", "odd", "even"]

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(simple_predicate)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        # Should keep only even numbers
        expected_packets = [2, 4, 6]
        expected_tags = ["even", "even", "even"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_none_match(self, sample_packets, sample_tags):
        """Test filter when no packets match."""

        def never_matches(packet, tag):
            return False

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        filter_mapper = Filter(never_matches)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)
        assert len(result) == 0

    def test_filter_all_match(self, sample_packets, sample_tags):
        """Test filter when all packets match."""

        def always_matches(packet, tag):
            return True

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        filter_mapper = Filter(always_matches)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == sample_tags

    def test_filter_empty_stream(self, simple_predicate):
        """Test filter with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        filter_mapper = Filter(simple_predicate)
        filtered_stream = filter_mapper(empty_stream)

        result = list(filtered_stream)
        assert len(result) == 0

    def test_filter_string_predicate(self):
        """Test filter with string-based predicate."""
        packets = ["apple", "banana", "cherry", "date", "elderberry"]
        tags = ["fruit1", "fruit2", "fruit3", "fruit4", "fruit5"]

        def starts_with_vowel(packet, tag):
            return isinstance(packet, str) and packet[0].lower() in "aeiou"

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(starts_with_vowel)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = ["apple", "elderberry"]
        expected_tags = ["fruit1", "fruit5"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_tag_based_predicate(self):
        """Test filter using tag information."""
        packets = [10, 20, 30, 40, 50]
        tags = ["small", "medium", "large", "huge", "enormous"]

        def tag_length_filter(packet, tag):
            return len(tag) <= 5

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(tag_length_filter)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = [10, 40]  # "small" and "huge" have <= 5 chars
        expected_tags = ["small", "huge"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_complex_predicate(self):
        """Test filter with complex predicate."""
        packets = [
            {"value": 5, "type": "A", "active": True},
            {"value": 15, "type": "B", "active": False},
            {"value": 25, "type": "A", "active": True},
            {"value": 35, "type": "C", "active": True},
            {"value": 45, "type": "A", "active": False},
        ]
        tags = ["item1", "item2", "item3", "item4", "item5"]

        def complex_predicate(packet, tag):
            return (
                isinstance(packet, dict)
                and packet.get("type") == "A"
                and packet.get("active", False)
                and packet.get("value", 0) > 10
            )

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(complex_predicate)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        # Only the third item matches all conditions
        expected_packets = [{"value": 25, "type": "A", "active": True}]
        expected_tags = ["item3"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_with_none_packets(self):
        """Test filter with None packets."""
        packets = [None, "data", None, "more_data", None]
        tags = ["empty1", "full1", "empty2", "full2", "empty3"]

        def not_none(packet, tag):
            return packet is not None

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(not_none)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = ["data", "more_data"]
        expected_tags = ["full1", "full2"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_preserves_packet_types(self):
        """Test that filter preserves packet types."""
        packets = [PacketType("data1"), [1, 2, 3], {"key": "value"}, "string", 42]
        tags = ["type1", "type2", "type3", "type4", "type5"]

        def is_container(packet, tag):
            return isinstance(packet, (list, dict))

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(is_container)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = [[1, 2, 3], {"key": "value"}]
        expected_tags = ["type2", "type3"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags
        assert isinstance(actual_packets[0], list)
        assert isinstance(actual_packets[1], dict)

    def test_filter_maintains_order(self):
        """Test that filter maintains packet order."""
        packets = [f"packet_{i}" for i in range(20)]
        tags = [f"tag_{i}" for i in range(20)]

        def keep_even_indices(packet, tag):
            # Extract index from packet name
            index = int(packet.split("_")[1])
            return index % 2 == 0

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(keep_even_indices)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = [f"packet_{i}" for i in range(0, 20, 2)]
        expected_tags = [f"tag_{i}" for i in range(0, 20, 2)]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_filter_predicate_exception(self, sample_packets, sample_tags):
        """Test filter when predicate raises exception."""

        def error_predicate(packet, tag):
            if packet == sample_packets[1]:  # Error on second packet
                raise ValueError("Predicate error")
            return True

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        filter_mapper = Filter(error_predicate)
        filtered_stream = filter_mapper(stream)

        # Should propagate the exception
        with pytest.raises(ValueError):
            list(filtered_stream)

    def test_filter_with_lambda(self):
        """Test filter with lambda predicate."""
        packets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tags = [f"num_{i}" for i in packets]

        stream = SyncStreamFromLists(packets, tags)
        filter_mapper = Filter(lambda p, t: p % 3 == 0)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        expected_packets = [3, 6, 9]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_filter_chaining(self):
        """Test chaining multiple filter operations."""
        packets = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        tags = [f"num_{i}" for i in packets]

        stream = SyncStreamFromLists(packets, tags)

        # First filter: keep even numbers
        filter1 = Filter(lambda p, t: p % 2 == 0)
        stream1 = filter1(stream)

        # Second filter: keep numbers > 4
        filter2 = Filter(lambda p, t: p > 4)
        stream2 = filter2(stream1)

        result = list(stream2)

        expected_packets = [6, 8, 10]  # Even numbers > 4
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_filter_with_generator_stream(self):
        """Test filter with generator-based stream."""

        def packet_generator():
            for i in range(20):
                yield i, f"tag_{i}"

        from orcabridge.stream import SyncStreamFromGenerator

        stream = SyncStreamFromGenerator(packet_generator())

        def is_prime(packet, tag):
            if packet < 2:
                return False
            for i in range(2, int(packet**0.5) + 1):
                if packet % i == 0:
                    return False
            return True

        filter_mapper = Filter(is_prime)
        filtered_stream = filter_mapper(stream)

        result = list(filtered_stream)

        # Prime numbers under 20: 2, 3, 5, 7, 11, 13, 17, 19
        expected_packets = [2, 3, 5, 7, 11, 13, 17, 19]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_filter_pickle(self):
        """Test that Filter mapper is pickleable."""
        import pickle
        from orcabridge.mappers import Filter

        def is_even(tag, packet):
            return packet % 2 == 0

        filter_mapper = Filter(is_even)
        pickled = pickle.dumps(filter_mapper)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, Filter)
        assert unpickled.__class__.__name__ == "Filter"
