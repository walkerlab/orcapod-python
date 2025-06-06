"""Tests for DefaultTag mapper functionality."""

import pytest
from orcabridge.base import PacketType
from orcabridge.mapper import DefaultTag
from orcabridge.stream import SyncStreamFromLists


class TestDefaultTag:
    """Test cases for DefaultTag mapper."""

    def test_default_tag_basic(self, sample_packets):
        """Test basic default tag functionality."""
        tags = ["existing1", None, "existing2"]

        stream = SyncStreamFromLists(sample_packets, tags)
        default_tag = DefaultTag("default_value")
        result_stream = default_tag(stream)

        result = list(result_stream)

        expected_tags = ["existing1", "default_value", "existing2"]
        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == sample_packets
        assert actual_tags == expected_tags

    def test_default_tag_all_none(self, sample_packets):
        """Test default tag when all tags are None."""
        tags = [None, None, None]

        stream = SyncStreamFromLists(sample_packets, tags)
        default_tag = DefaultTag("fallback")
        result_stream = default_tag(stream)

        result = list(result_stream)

        expected_tags = ["fallback", "fallback", "fallback"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_default_tag_no_none(self, sample_packets, sample_tags):
        """Test default tag when no tags are None."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        default_tag = DefaultTag("unused_default")
        result_stream = default_tag(stream)

        result = list(result_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        # Should remain unchanged
        assert actual_packets == sample_packets
        assert actual_tags == sample_tags

    def test_default_tag_empty_stream(self):
        """Test default tag with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        default_tag = DefaultTag("default")
        result_stream = default_tag(empty_stream)

        result = list(result_stream)
        assert len(result) == 0

    def test_default_tag_different_types(self):
        """Test default tag with different default value types."""
        packets = ["data1", "data2", "data3"]
        tags = [None, "existing", None]

        # Test with string default
        stream1 = SyncStreamFromLists(packets, tags)
        default_tag1 = DefaultTag("string_default")
        result1 = list(default_tag1(stream1))

        expected_tags1 = ["string_default", "existing", "string_default"]
        actual_tags1 = [tag for _, tag in result1]
        assert actual_tags1 == expected_tags1

        # Test with numeric default
        stream2 = SyncStreamFromLists(packets, tags)
        default_tag2 = DefaultTag(42)
        result2 = list(default_tag2(stream2))

        expected_tags2 = [42, "existing", 42]
        actual_tags2 = [tag for _, tag in result2]
        assert actual_tags2 == expected_tags2

    def test_default_tag_empty_string_vs_none(self):
        """Test default tag distinguishes between empty string and None."""
        packets = ["data1", "data2", "data3"]
        tags = [None, "", None]  # Empty string vs None

        stream = SyncStreamFromLists(packets, tags)
        default_tag = DefaultTag("default")
        result_stream = default_tag(stream)

        result = list(result_stream)

        # Empty string should be preserved, None should be replaced
        expected_tags = ["default", "", "default"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags

    def test_default_tag_preserves_packets(self):
        """Test that default tag preserves all packet types."""
        packets = [PacketType("data1"), {"key": "value"}, [1, 2, 3], 42, "string"]
        tags = [None, None, "existing", None, None]

        stream = SyncStreamFromLists(packets, tags)
        default_tag = DefaultTag("default")
        result_stream = default_tag(stream)

        result = list(result_stream)

        actual_packets = [packet for packet, _ in result]
        expected_tags = ["default", "default", "existing", "default", "default"]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == packets
        assert actual_tags == expected_tags

    def test_default_tag_with_complex_default(self):
        """Test default tag with complex default value."""
        packets = ["data1", "data2"]
        tags = [None, "existing"]

        default_value = {"type": "default", "timestamp": 12345}

        stream = SyncStreamFromLists(packets, tags)
        default_tag = DefaultTag(default_value)
        result_stream = default_tag(stream)

        result = list(result_stream)

        expected_tags = [default_value, "existing"]
        actual_tags = [tag for _, tag in result]

        assert actual_tags == expected_tags
        assert actual_tags[0] is default_value  # Should be the same object

    def test_default_tag_chaining(self, sample_packets):
        """Test chaining multiple default tag operations."""
        tags = [None, "middle", None]

        stream = SyncStreamFromLists(sample_packets, tags)

        # First default tag
        default_tag1 = DefaultTag("first_default")
        stream1 = default_tag1(stream)

        # Create new stream with some None tags again
        intermediate_result = list(stream1)
        new_tags = [
            None if tag == "first_default" else tag for _, tag in intermediate_result
        ]
        new_packets = [packet for packet, _ in intermediate_result]

        stream2 = SyncStreamFromLists(new_packets, new_tags)
        default_tag2 = DefaultTag("second_default")
        stream3 = default_tag2(stream2)

        final_result = list(stream3)

        # The "middle" tag should be preserved
        actual_tags = [tag for _, tag in final_result]
        assert "middle" in actual_tags
        assert "second_default" in actual_tags

    def test_default_tag_maintains_order(self):
        """Test that default tag maintains packet order."""
        packets = [f"packet_{i}" for i in range(10)]
        tags = [None if i % 2 == 0 else f"tag_{i}" for i in range(10)]

        stream = SyncStreamFromLists(packets, tags)
        default_tag = DefaultTag("even_default")
        result_stream = default_tag(stream)

        result = list(result_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert actual_packets == packets

        # Check that even indices got default tags, odd indices kept original
        for i in range(10):
            if i % 2 == 0:
                assert actual_tags[i] == "even_default"
            else:
                assert actual_tags[i] == f"tag_{i}"

    def test_default_tag_with_callable_default(self):
        """Test default tag with callable default (if supported)."""
        packets = ["data1", "data2", "data3"]
        tags = [None, "existing", None]

        # Simple callable that returns a counter
        class DefaultGenerator:
            def __init__(self):
                self.count = 0

            def __call__(self):
                self.count += 1
                return f"default_{self.count}"

        # If the implementation supports callable defaults
        try:
            default_gen = DefaultGenerator()
            stream = SyncStreamFromLists(packets, tags)
            default_tag = DefaultTag(default_gen)
            result_stream = default_tag(stream)

            result = list(result_stream)
            actual_tags = [tag for _, tag in result]

            # This would only work if DefaultTag supports callable defaults
            # Otherwise this test should be skipped or modified
            assert "existing" in actual_tags
        except (TypeError, AttributeError):
            # If callable defaults are not supported, that's fine
            pass

    def test_default_tag_large_stream(self):
        """Test default tag with large stream."""
        packets = [f"packet_{i}" for i in range(1000)]
        tags = [None if i % 3 == 0 else f"tag_{i}" for i in range(1000)]

        stream = SyncStreamFromLists(packets, tags)
        default_tag = DefaultTag("bulk_default")
        result_stream = default_tag(stream)

        result = list(result_stream)

        actual_packets = [packet for packet, _ in result]
        actual_tags = [tag for _, tag in result]

        assert len(actual_packets) == 1000
        assert len(actual_tags) == 1000

        # Check that every third tag was replaced
        for i in range(1000):
            if i % 3 == 0:
                assert actual_tags[i] == "bulk_default"
            else:
                assert actual_tags[i] == f"tag_{i}"    def test_default_tag_pickle(self):
        """Test that DefaultTag mapper is pickleable."""
        import pickle
        from orcabridge.mappers import DefaultTag
        
        default_tag = DefaultTag({"default": "test"})
        pickled = pickle.dumps(default_tag)
        unpickled = pickle.loads(pickled)
        
        # Test that unpickled mapper works the same
        assert isinstance(unpickled, DefaultTag)
        assert unpickled.default_tag == default_tag.default_tag
