"""Tests for utility functions tag() and packet()."""

from orcabridge.mappers import tag, packet
from orcabridge.streams import SyncStreamFromLists


class TestUtilityFunctions:
    """Test cases for tag() and packet() utility functions."""

    def test_tag_function_basic(self):
        """Test basic tag() function functionality."""
        tags = [
            {"old_key": "value1", "other": "data1"},
            {"old_key": "value2", "other": "data2"},
        ]
        packets = [
            {"data": "packet1"},
            {"data": "packet2"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        tag_mapper = tag({"old_key": "new_key"})
        transformed_stream = tag_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 2
        for (result_tag, result_packet), original_packet in zip(results, packets):
            # Tag should be transformed
            assert "new_key" in result_tag
            assert "old_key" not in result_tag  # old key dropped by default
            assert result_tag["new_key"] in ["value1", "value2"]

            # Packet should be unchanged
            assert result_packet == original_packet

    def test_tag_function_keep_unmapped(self):
        """Test tag() function with drop_unmapped=False."""
        tags = [
            {"old_key": "value1", "keep_this": "data1"},
        ]
        packets = [
            {"data": "packet1"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        tag_mapper = tag({"old_key": "new_key"}, drop_unmapped=False)
        transformed_stream = tag_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # Should have both mapped and unmapped keys
        assert result_tag["new_key"] == "value1"
        assert result_tag["keep_this"] == "data1"

    def test_packet_function_basic(self):
        """Test basic packet() function functionality."""
        tags = [
            {"tag_data": "tag1"},
            {"tag_data": "tag2"},
        ]
        packets = [
            {"old_key": "value1", "other": "data1"},
            {"old_key": "value2", "other": "data2"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        packet_mapper = packet({"old_key": "new_key"})
        transformed_stream = packet_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 2
        for (result_tag, result_packet), original_tag in zip(results, tags):
            # Tag should be unchanged
            assert result_tag == original_tag

            # Packet should be transformed
            assert "new_key" in result_packet
            assert "old_key" not in result_packet  # old key dropped by default
            assert result_packet["new_key"] in ["value1", "value2"]

    def test_packet_function_keep_unmapped(self):
        """Test packet() function with drop_unmapped=False."""
        tags = [
            {"tag_data": "tag1"},
        ]
        packets = [
            {"old_key": "value1", "keep_this": "data1"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        packet_mapper = packet({"old_key": "new_key"}, drop_unmapped=False)
        transformed_stream = packet_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # Should have both mapped and unmapped keys
        assert result_packet["new_key"] == "value1"
        assert result_packet["keep_this"] == "data1"

    def test_tag_function_empty_mapping(self):
        """Test tag() function with empty mapping."""
        tags = [
            {"key1": "value1", "key2": "value2"},
        ]
        packets = [
            {"data": "packet1"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        tag_mapper = tag({})  # Empty mapping
        transformed_stream = tag_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # With empty mapping and drop_unmapped=True (default), all keys should be dropped
        assert result_tag == {}
        assert result_packet == packets[0]  # Packet unchanged

    def test_packet_function_empty_mapping(self):
        """Test packet() function with empty mapping."""
        tags = [
            {"tag_data": "tag1"},
        ]
        packets = [
            {"key1": "value1", "key2": "value2"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        packet_mapper = packet({})  # Empty mapping
        transformed_stream = packet_mapper(stream)

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # With empty mapping and drop_unmapped=True (default), all keys should be dropped
        assert result_tag == tags[0]  # Tag unchanged
        assert result_packet == {}

    def test_tag_function_chaining(self):
        """Test chaining multiple tag() transformations."""
        tags = [
            {"a": "value1", "b": "value2", "c": "value3"},
        ]
        packets = [
            {"data": "packet1"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)

        # Chain transformations
        tag_mapper1 = tag({"a": "new_a"}, drop_unmapped=False)
        tag_mapper2 = tag({"b": "new_b"}, drop_unmapped=False)

        transformed_stream = tag_mapper2(tag_mapper1(stream))

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # Should have transformations from both mappers
        assert result_tag["new_a"] == "value1"
        assert result_tag["new_b"] == "value2"
        assert result_tag["c"] == "value3"  # Unchanged

    def test_packet_function_chaining(self):
        """Test chaining multiple packet() transformations."""
        tags = [
            {"tag_data": "tag1"},
        ]
        packets = [
            {"a": "value1", "b": "value2", "c": "value3"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)

        # Chain transformations
        packet_mapper1 = packet({"a": "new_a"}, drop_unmapped=False)
        packet_mapper2 = packet({"b": "new_b"}, drop_unmapped=False)

        transformed_stream = packet_mapper2(packet_mapper1(stream))

        results = list(transformed_stream)

        assert len(results) == 1
        result_tag, result_packet = results[0]

        # Should have transformations from both mappers
        assert result_packet["new_a"] == "value1"
        assert result_packet["new_b"] == "value2"
        assert result_packet["c"] == "value3"  # Unchanged

    def test_utility_functions_pickle(self):
        """Test that utility functions tag() and packet() are pickleable."""
        import pickle

        # Test tag() function
        tag_mapper = tag({"old_key": "new_key"})
        pickled_tag = pickle.dumps(tag_mapper)
        unpickled_tag = pickle.loads(pickled_tag)

        # Test that unpickled tag mapper works
        assert callable(unpickled_tag)

        # Test packet() function
        packet_mapper = packet({"old_key": "new_key"})
        pickled_packet = pickle.dumps(packet_mapper)
        unpickled_packet = pickle.loads(pickled_packet)

        # Test that unpickled packet mapper works
        assert callable(unpickled_packet)

    def test_utility_functions_with_complex_streams(self, sample_stream):
        """Test utility functions with complex streams from fixtures."""
        # Test tag() with sample stream
        tag_mapper = tag({"file_name": "filename"}, drop_unmapped=False)
        transformed_stream = tag_mapper(sample_stream)

        results = list(transformed_stream)

        for result_tag, _ in results:
            assert "filename" in result_tag
            assert result_tag["filename"] in ["day1", "day2", "day3"]
            assert "session" in result_tag  # Kept because drop_unmapped=False

        # Test packet() with sample stream
        packet_mapper = packet({"txt_file": "text_file"}, drop_unmapped=False)
        transformed_stream = packet_mapper(sample_stream)

        results = list(transformed_stream)

        for _, result_packet in results:
            assert "text_file" in result_packet
            assert "data" in result_packet["text_file"]
            assert "metadata" in result_packet  # Kept because drop_unmapped=False
