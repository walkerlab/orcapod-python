"""Tests for MapTags mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import MapTags
from orcabridge.stream import SyncStreamFromLists


class TestMapTags:
    """Test cases for MapTags mapper."""

    def test_map_tags_basic(self, sample_packets, sample_tags):
        """Test basic map tags functionality."""

        def add_prefix(tag):
            return f"mapped_{tag}"

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_tags = MapTags(add_prefix)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = [f"mapped_{t}" for t in sample_tags]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        # Packets should be unchanged, tags transformed
        assert actual_packets == sample_packets
        assert actual_tags == expected_tags

    def test_map_tags_type_conversion(self, sample_packets):
        """Test map tags with type conversion."""
        tags = ["1", "2", "3"]

        def str_to_int(tag):
            return int(tag)

        stream = SyncStreamFromLists(sample_packets, tags)
        map_tags = MapTags(str_to_int)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = [1, 2, 3]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags
        assert all(isinstance(t, int) for t in actual_tags)

    def test_map_tags_complex_transformation(self):
        """Test map tags with complex transformation."""
        packets = ["data1", "data2", "data3"]
        tags = [
            {"type": "string", "length": 5},
            {"type": "string", "length": 5},
            {"type": "string", "length": 5},
        ]

        def extract_type(tag):
            if isinstance(tag, dict):
                return tag.get("type", "unknown")
            return str(tag)

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(extract_type)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = ["string", "string", "string"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_map_tags_identity_function(self, sample_packets, sample_tags):
        """Test map tags with identity function."""

        def identity(tag):
            return tag

        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_tags = MapTags(identity)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == sample_tags

    def test_map_tags_empty_stream(self):
        """Test map tags with empty stream."""

        def dummy_transform(tag):
            return f"transformed_{tag}"

        empty_stream = SyncStreamFromLists([], [])
        map_tags = MapTags(dummy_transform)
        mapped_stream = map_tags(empty_stream)

        result = list(mapped_stream)
        assert len(result) == 0

    def test_map_tags_with_none_values(self, sample_packets):
        """Test map tags with None values."""
        tags = ["tag1", None, "tag3"]

        def handle_none(tag):
            return "NULL_TAG" if tag is None else tag.upper()

        stream = SyncStreamFromLists(sample_packets, tags)
        map_tags = MapTags(handle_none)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = ["TAG1", "NULL_TAG", "TAG3"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_map_tags_exception_handling(self, sample_packets):
        """Test map tags when transformation function raises exception."""
        tags = ["valid", "also_valid", 123]  # 123 will cause error in upper()

        def to_upper(tag):
            return tag.upper()  # Will fail on integer

        stream = SyncStreamFromLists(sample_packets, tags)
        map_tags = MapTags(to_upper)
        mapped_stream = map_tags(stream)

        # Should raise exception when processing integer tag
        with pytest.raises(AttributeError):
            list(mapped_stream)

    def test_map_tags_preserves_packets(self):
        """Test that map tags preserves all packet types."""
        packets = [PacketType("data1"), {"key": "value"}, [1, 2, 3], 42, "string"]
        tags = ["type1", "type2", "type3", "type4", "type5"]

        def add_suffix(tag):
            return f"{tag}_processed"

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(add_suffix)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        actual_packets = [packet for packet, _ in result]
        expected_tags = [f"{t}_processed" for t in tags]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == packets
        assert actual_tags == expected_tags

    def test_map_tags_maintains_order(self):
        """Test that map tags maintains packet order."""
        packets = [f"packet_{i}" for i in range(100)]
        tags = [f"tag_{i}" for i in range(100)]

        def reverse_tag(tag):
            return tag[::-1]  # Reverse the string

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(reverse_tag)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = [f"{i}_gat" for i in range(100)]  # "tag_i" reversed
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == packets
        assert actual_tags == expected_tags

    def test_map_tags_with_lambda(self, sample_packets, sample_tags):
        """Test map tags with lambda function."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        map_tags = MapTags(lambda t: f"λ({t})")
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = [f"λ({t})" for t in sample_tags]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_map_tags_chaining(self, sample_packets, sample_tags):
        """Test chaining multiple map tags operations."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)

        # First transformation
        map1 = MapTags(lambda t: f"first_{t}")
        stream1 = map1(stream)

        # Second transformation
        map2 = MapTags(lambda t: f"second_{t}")
        stream2 = map2(stream1)

        result = list(stream2)

        expected_tags = [f"second_first_{t}" for t in sample_tags]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == expected_tags

    def test_map_tags_stateful_transformation(self):
        """Test map tags with stateful transformation."""
        packets = ["a", "b", "c", "d", "e"]
        tags = ["tag1", "tag2", "tag3", "tag4", "tag5"]

        class TagCounter:
            def __init__(self):
                self.count = 0

            def transform(self, tag):
                self.count += 1
                return f"{tag}_#{self.count}"

        counter = TagCounter()
        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(counter.transform)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = ["tag1_#1", "tag2_#2", "tag3_#3", "tag4_#4", "tag5_#5"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_map_tags_with_complex_types(self):
        """Test map tags with complex tag types."""
        packets = ["data1", "data2", "data3"]
        tags = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "A"},
        ]

        def extract_category(tag):
            if isinstance(tag, dict):
                return f"cat_{tag.get('category', 'unknown')}"
            return str(tag)

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(extract_category)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        expected_tags = ["cat_A", "cat_B", "cat_A"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_map_tags_preserves_tag_references(self):
        """Test that map tags doesn't break tag references when not needed."""
        packets = ["data1", "data2"]
        shared_tag = {"shared": "reference"}
        tags = [shared_tag, shared_tag]

        def conditional_transform(tag):
            # Only transform if it's a string
            if isinstance(tag, str):
                return f"transformed_{tag}"
            return tag  # Keep dict unchanged

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(conditional_transform)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        actual_tags = [tag for _, tag in result]

        # Both tags should still reference the same object
        assert actual_tags[0] is shared_tag
        assert actual_tags[1] is shared_tag
        assert actual_tags[0] is actual_tags[1]

    def test_map_tags_large_stream(self):
        """Test map tags with large stream."""
        packets = [f"packet_{i}" for i in range(1000)]
        tags = [f"tag_{i}" for i in range(1000)]

        def add_hash(tag):
            return f"{tag}_{hash(tag) % 1000}"

        stream = SyncStreamFromLists(packets, tags)
        map_tags = MapTags(add_hash)
        mapped_stream = map_tags(stream)

        result = list(mapped_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert len(actual_packets) == 1000
        assert len(actual_tags) == 1000
        assert actual_packets == packets

        # All tags should have been transformed
        assert all(
            "_" in tag and tag != f"tag_{i}" for i, tag in enumerate(actual_tags)
        )

    def test_map_tags_pickle(self):
        """Test that MapTags mapper is pickleable."""
        import pickle
        from orcabridge.mappers import MapTags

        # MapTags takes a key mapping, not a transformation function
        key_map = {"old_tag": "new_tag", "category": "type"}
        map_tags = MapTags(key_map)
        pickled = pickle.dumps(map_tags)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, MapTags)
        assert unpickled.key_map == map_tags.key_map
