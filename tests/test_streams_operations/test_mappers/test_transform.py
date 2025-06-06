"""Tests for Transform mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import Transform
from orcabridge.stream import SyncStreamFromLists


class TestTransform:
    """Test cases for Transform mapper."""

    def test_transform_basic(self, simple_transform):
        """Test basic transform functionality."""
        packets = ["hello", "world", "test"]
        tags = ["greeting", "noun", "action"]

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(simple_transform)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = ["HELLO", "WORLD", "TEST"]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == tags  # Tags should be preserved

    def test_transform_with_tag_modification(self):
        """Test transform that modifies both packet and tag."""
        packets = [1, 2, 3, 4, 5]
        tags = ["num1", "num2", "num3", "num4", "num5"]

        def double_and_prefix_tag(packet, tag):
            return packet * 2, f"doubled_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(double_and_prefix_tag)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [2, 4, 6, 8, 10]
        expected_tags = [
            "doubled_num1",
            "doubled_num2",
            "doubled_num3",
            "doubled_num4",
            "doubled_num5",
        ]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_packet_only(self, sample_packets, sample_tags):
        """Test transform that only modifies packets."""

        def add_prefix(packet, tag):
            return f"transformed_{packet}", tag

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        transform_mapper = Transform(add_prefix)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [f"transformed_{p}" for p in sample_packets]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == sample_tags

    def test_transform_tag_only(self, sample_packets, sample_tags):
        """Test transform that only modifies tags."""

        def add_tag_suffix(packet, tag):
            return packet, f"{tag}_processed"

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        transform_mapper = Transform(add_tag_suffix)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_tags = [f"{t}_processed" for t in sample_tags]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == expected_tags

    def test_transform_empty_stream(self):
        """Test transform with empty stream."""

        def dummy_transform(packet, tag):
            return packet, tag

        empty_stream = SyncStreamFromLists([], [])
        transform_mapper = Transform(dummy_transform)
        transformed_stream = transform_mapper(empty_stream)

        result = list(transformed_stream)
        assert len(result) == 0

    def test_transform_type_conversion(self):
        """Test transform with type conversion."""
        packets = ["1", "2", "3", "4", "5"]
        tags = ["str1", "str2", "str3", "str4", "str5"]

        def str_to_int_with_tag(packet, tag):
            return int(packet), f"int_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(str_to_int_with_tag)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [1, 2, 3, 4, 5]
        expected_tags = ["int_str1", "int_str2", "int_str3", "int_str4", "int_str5"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags
        assert all(isinstance(p, int) for p in actual_packets)

    def test_transform_complex_data(self):
        """Test transform with complex data structures."""
        packets = [
            {"name": "alice", "age": 25},
            {"name": "bob", "age": 30},
            {"name": "charlie", "age": 35},
        ]
        tags = ["person1", "person2", "person3"]

        def enrich_person_data(packet, tag):
            enriched = packet.copy()
            enriched["category"] = "adult" if packet["age"] >= 30 else "young"
            return enriched, f"enriched_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(enrich_person_data)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [
            {"name": "alice", "age": 25, "category": "young"},
            {"name": "bob", "age": 30, "category": "adult"},
            {"name": "charlie", "age": 35, "category": "adult"},
        ]
        expected_tags = ["enriched_person1", "enriched_person2", "enriched_person3"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_with_none_values(self):
        """Test transform with None values."""
        packets = [1, None, 3, None, 5]
        tags = ["num1", "null1", "num3", "null2", "num5"]

        def handle_none_transform(packet, tag):
            if packet is None:
                return "MISSING", f"missing_{tag}"
            else:
                return packet * 2, f"doubled_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(handle_none_transform)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [2, "MISSING", 6, "MISSING", 10]
        expected_tags = [
            "doubled_num1",
            "missing_null1",
            "doubled_num3",
            "missing_null2",
            "doubled_num5",
        ]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_preserves_order(self):
        """Test that transform preserves packet order."""
        packets = [f"packet_{i}" for i in range(100)]
        tags = [f"tag_{i}" for i in range(100)]

        def add_index(packet, tag):
            index = int(packet.split("_")[1])
            return f"indexed_{index}_{packet}", f"indexed_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(add_index)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [f"indexed_{i}_packet_{i}" for i in range(100)]
        expected_tags = [f"indexed_tag_{i}" for i in range(100)]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_exception_handling(self):
        """Test transform when transformation function raises exception."""
        packets = [1, 2, "invalid", 4]
        tags = ["num1", "num2", "str1", "num4"]

        def divide_transform(packet, tag):
            return 10 / packet, f"divided_{tag}"  # Will fail on "invalid"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(divide_transform)
        transformed_stream = transform_mapper(stream)

        # Should raise exception when processing "invalid"
        with pytest.raises(TypeError):
            list(transformed_stream)

    def test_transform_with_lambda(self):
        """Test transform with lambda function."""
        packets = [1, 2, 3, 4, 5]
        tags = ["a", "b", "c", "d", "e"]

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(lambda p, t: (p**2, t.upper()))
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [1, 4, 9, 16, 25]
        expected_tags = ["A", "B", "C", "D", "E"]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_chaining(self):
        """Test chaining multiple transform operations."""
        packets = [1, 2, 3, 4, 5]
        tags = ["num1", "num2", "num3", "num4", "num5"]

        stream = SyncStreamFromLists(packets, tags)

        # First transformation: double the packet
        transform1 = Transform(lambda p, t: (p * 2, f"doubled_{t}"))
        stream1 = transform1(stream)

        # Second transformation: add 10 to packet
        transform2 = Transform(lambda p, t: (p + 10, f"added_{t}"))
        stream2 = transform2(stream1)

        result = list(stream2)

        expected_packets = [12, 14, 16, 18, 20]  # (original * 2) + 10
        expected_tags = [
            "added_doubled_num1",
            "added_doubled_num2",
            "added_doubled_num3",
            "added_doubled_num4",
            "added_doubled_num5",
        ]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_with_packet_type(self):
        """Test transform with PacketType objects."""
        packets = [PacketType("data1"), PacketType("data2")]
        tags = ["type1", "type2"]

        def extract_and_modify(packet, tag):
            data = str(packet)  # Convert to string
            return f"extracted_{data}", f"processed_{tag}"

        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(extract_and_modify)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert len(actual_packets) == 2
        assert all("extracted_" in p for p in actual_packets)
        assert actual_tags == ["processed_type1", "processed_type2"]

    def test_transform_stateful(self):
        """Test transform with stateful transformation."""
        packets = [1, 2, 3, 4, 5]
        tags = ["n1", "n2", "n3", "n4", "n5"]

        class StatefulTransform:
            def __init__(self):
                self.counter = 0

            def transform(self, packet, tag):
                self.counter += 1
                return (packet + self.counter, f"{tag}_step_{self.counter}")

        stateful = StatefulTransform()
        stream = SyncStreamFromLists(packets, tags)
        transform_mapper = Transform(stateful.transform)
        transformed_stream = transform_mapper(stream)

        result = list(transformed_stream)

        expected_packets = [2, 4, 6, 8, 10]  # packet + step_number
        expected_tags = [
            "n1_step_1",
            "n2_step_2",
            "n3_step_3",
            "n4_step_4",
            "n5_step_5",
        ]

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == expected_tags

    def test_transform_pickle(self):
        """Test that Transform mapper is pickleable."""
        import pickle
        from orcabridge.mappers import Transform

        def add_prefix(tag, packet):
            new_tag = {**tag, "prefix": "test"}
            new_packet = {**packet, "processed": True}
            return new_tag, new_packet

        transform = Transform(add_prefix)
        pickled = pickle.dumps(transform)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, Transform)
        assert unpickled.__class__.__name__ == "Transform"
