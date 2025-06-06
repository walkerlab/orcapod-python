"""Tests for MapPackets mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import MapPackets
from orcabridge.stream import SyncStreamFromLists


class TestMapPackets:
    """Test cases for MapPackets mapper."""

    def test_map_packets_basic(self, sample_packets, sample_tags):
        """Test basic map packets functionality."""

        def add_suffix(packet):
            return f"{packet}_mapped"

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_packets = MapPackets(add_suffix)
        mapped_stream = map_packets(stream)

        result_packets = []
        result_tags = []
        for packet, tag in mapped_stream:
            result_packets.append(packet)
            result_tags.append(tag)

        # Packets should be transformed, tags unchanged
        expected_packets = [f"{p}_mapped" for p in sample_packets]
        assert result_packets == expected_packets
        assert result_tags == sample_tags

    def test_map_packets_numeric_transformation(self):
        """Test map packets with numeric transformation."""
        packets = [1, 2, 3, 4, 5]
        tags = ["num1", "num2", "num3", "num4", "num5"]

        def square(packet):
            return packet**2

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(square)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [1, 4, 9, 16, 25]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == expected_packets
        assert actual_tags == tags

    def test_map_packets_type_conversion(self):
        """Test map packets with type conversion."""
        packets = ["1", "2", "3", "4"]
        tags = ["str1", "str2", "str3", "str4"]

        def str_to_int(packet):
            return int(packet)

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(str_to_int)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [1, 2, 3, 4]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets
        assert all(isinstance(p, int) for p in actual_packets)

    def test_map_packets_complex_transformation(self):
        """Test map packets with complex data transformation."""
        packets = [
            {"name": "alice", "age": 25},
            {"name": "bob", "age": 30},
            {"name": "charlie", "age": 35},
        ]
        tags = ["person1", "person2", "person3"]

        def create_description(packet):
            return f"{packet['name']} is {packet['age']} years old"

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(create_description)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [
            "alice is 25 years old",
            "bob is 30 years old",
            "charlie is 35 years old",
        ]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_map_packets_identity_function(self, sample_packets, sample_tags):
        """Test map packets with identity function."""

        def identity(packet):
            return packet

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_packets = MapPackets(identity)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == sample_tags

    def test_map_packets_empty_stream(self):
        """Test map packets with empty stream."""

        def dummy_transform(packet):
            return packet * 2

        empty_stream = SyncStreamFromLists([], [])
        map_packets = MapPackets(dummy_transform)
        mapped_stream = map_packets(empty_stream)

        result = list(mapped_stream)
        assert len(result) == 0

    def test_map_packets_with_none_values(self):
        """Test map packets with None values."""
        packets = [1, None, 3, None, 5]
        tags = ["num1", "null1", "num3", "null2", "num5"]

        def handle_none(packet):
            return 0 if packet is None else packet * 2

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(handle_none)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [2, 0, 6, 0, 10]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_map_packets_exception_handling(self):
        """Test map packets when transformation function raises exception."""
        packets = [1, 2, "invalid", 4]
        tags = ["num1", "num2", "str1", "num4"]

        def divide_by_packet(packet):
            return 10 / packet  # Will fail on "invalid"

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(divide_by_packet)
        mapped_stream = map_packets(stream)

        # Should raise exception when processing "invalid"
        with pytest.raises(TypeError):
            list(mapped_stream)

    def test_map_packets_preserves_order(self):
        """Test that map packets preserves packet order."""
        packets = [f"packet_{i}" for i in range(100)]
        tags = [f"tag_{i}" for i in range(100)]

        def add_prefix(packet):
            return f"mapped_{packet}"

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(add_prefix)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [f"mapped_packet_{i}" for i in range(100)]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_map_packets_with_lambda(self, sample_packets, sample_tags):
        """Test map packets with lambda function."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_packets = MapPackets(lambda x: f"λ({x})")
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [f"λ({p})" for p in sample_packets]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_map_packets_chaining(self, sample_packets, sample_tags):
        """Test chaining multiple map packets operations."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)

        # First transformation
        map1 = MapPackets(lambda x: f"first_{x}")
        stream1 = map1(stream)

        # Second transformation
        map2 = MapPackets(lambda x: f"second_{x}")
        stream2 = map2(stream1)

        result = list(stream2)

        expected_packets = [f"second_first_{p}" for p in sample_packets]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets

    def test_map_packets_with_packet_type(self):
        """Test map packets with PacketType objects."""
        packets = [PacketType("data1"), PacketType("data2")]
        tags = ["type1", "type2"]

        def extract_data(packet):
            return packet.data if hasattr(packet, "data") else str(packet)

        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(extract_data)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)
        actual_packets = [packet for packet, _ in result]

        # Should extract string representation or data
        assert len(actual_packets) == 2
        assert all(isinstance(p, str) for p in actual_packets)

    def test_map_packets_stateful_transformation(self):
        """Test map packets with stateful transformation."""
        packets = [1, 2, 3, 4, 5]
        tags = ["n1", "n2", "n3", "n4", "n5"]

        class Counter:
            def __init__(self):
                self.count = 0

            def transform(self, packet):
                self.count += 1
                return (packet, self.count)

        counter = Counter()
        stream = SyncStreamFromLists(packets, tags)
        map_packets = MapPackets(counter.transform)
        mapped_stream = map_packets(stream)

        result = list(mapped_stream)

        expected_packets = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]
        actual_packets = [packet for packet, _ in result]

        assert actual_packets == expected_packets    def test_map_packets_pickle(self):
        """Test that MapPackets mapper is pickleable."""
        import pickle
        from orcabridge.mappers import MapPackets
        
        # MapPackets takes a key mapping, not a transformation function
        key_map = {"old_key": "new_key", "data": "value"}
        map_packets = MapPackets(key_map)
        pickled = pickle.dumps(map_packets)
        unpickled = pickle.loads(pickled)
        
        # Test that unpickled mapper works the same
        assert isinstance(unpickled, MapPackets)
        assert unpickled.key_map == map_packets.key_map
