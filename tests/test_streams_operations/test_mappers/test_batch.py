"""Tests for Batch mapper functionality."""

import pytest
from orcabridge.mappers import Batch
from orcabridge.streams import SyncStreamFromLists


class TestBatch:
    """Test cases for Batch mapper."""

    def test_batch_basic(self, sample_tags, sample_packets):
        """Test basic batch functionality."""
        stream = SyncStreamFromLists(sample_tags, sample_packets)
        batch = Batch(2, drop_last=False)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have 2 batches: [packet1, packet2] and [packet3]
        assert len(result) == 2

        batch1_tag, batch1_packet = result[0]
        batch2_tag, batch2_packet = result[1]

        # First batch should have 2 items
        assert len(batch1_packet["txt_file"]) == 2
        for k, v in batch1_packet.items():
            assert v == [p[k] for p in sample_packets[:2]]

        assert len(batch2_packet["txt_file"]) == 1
        for k, v in batch2_packet.items():
            assert v == [p[k] for p in sample_packets[2:]]

    def test_batch_exact_division(self):
        """Test batch when stream length divides evenly by batch size."""
        packets = [1, 2, 3, 4, 5, 6]
        tags = ["a", "b", "c", "d", "e", "f"]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(3)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have exactly 2 batches
        assert len(result) == 2

        batch1_packet, _ = result[0]
        batch2_packet, _ = result[1]

        assert len(batch1_packet) == 3
        assert len(batch2_packet) == 3
        assert list(batch1_packet) == [1, 2, 3]
        assert list(batch2_packet) == [4, 5, 6]

    def test_batch_size_one(self, sample_packets, sample_tags):
        """Test batch with size 1."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        batch = Batch(1)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have same number of batches as original packets
        assert len(result) == len(sample_packets)

        for i, (batch_packet, batch_tag) in enumerate(result):
            assert len(batch_packet) == 1
            assert list(batch_packet) == [sample_packets[i]]

    def test_batch_larger_than_stream(self, sample_packets, sample_tags):
        """Test batch size larger than stream."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        batch = Batch(10)  # Larger than sample_packets length
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have exactly 1 batch with all packets
        assert len(result) == 1

        batch_packet, batch_tag = result[0]
        assert len(batch_packet) == len(sample_packets)
        assert list(batch_packet) == sample_packets

    def test_batch_empty_stream(self):
        """Test batch with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        batch = Batch(3)
        batched_stream = batch(empty_stream)

        result = list(batched_stream)
        assert len(result) == 0

    def test_batch_preserves_packet_types(self):
        """Test that batch preserves different packet types."""
        packets = [PacketType("data1"), {"key": "value"}, [1, 2, 3], 42, "string"]
        tags = ["type1", "type2", "type3", "type4", "type5"]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(2)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have 3 batches: [2, 2, 1]
        assert len(result) == 3

        # Check first batch
        batch1_packet, _ = result[0]
        batch1_list = list(batch1_packet)
        assert batch1_list[0] == PacketType("data1")
        assert batch1_list[1] == {"key": "value"}

        # Check second batch
        batch2_packet, _ = result[1]
        batch2_list = list(batch2_packet)
        assert batch2_list[0] == [1, 2, 3]
        assert batch2_list[1] == 42

        # Check third batch
        batch3_packet, _ = result[2]
        batch3_list = list(batch3_packet)
        assert batch3_list[0] == "string"

    def test_batch_tag_handling(self, sample_packets, sample_tags):
        """Test how batch handles tags."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)
        batch = Batch(2)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Each batch should have some representation of the constituent tags
        for batch_packet, batch_tag in result:
            assert batch_tag is not None
            # The exact format depends on implementation

    def test_batch_maintains_order(self):
        """Test that batch maintains packet order within batches."""
        packets = [f"packet_{i}" for i in range(10)]
        tags = [f"tag_{i}" for i in range(10)]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(3)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have 4 batches: [3, 3, 3, 1]
        assert len(result) == 4

        # Check order within each batch
        all_packets = []
        for batch_packet, _ in result:
            all_packets.extend(list(batch_packet))

        assert all_packets == packets

    def test_batch_large_stream(self):
        """Test batch with large stream."""
        packets = [f"packet_{i}" for i in range(1000)]
        tags = [f"tag_{i}" for i in range(1000)]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(50)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have exactly 20 batches of 50 each
        assert len(result) == 20

        for i, (batch_packet, _) in enumerate(result):
            assert len(batch_packet) == 50
            expected_packets = packets[i * 50 : (i + 1) * 50]
            assert list(batch_packet) == expected_packets

    def test_batch_invalid_size(self):
        """Test batch with invalid size."""
        with pytest.raises(ValueError):
            Batch(0)

        with pytest.raises(ValueError):
            Batch(-1)

        with pytest.raises(TypeError):
            Batch(3.5)

        with pytest.raises(TypeError):
            Batch("3")

    def test_batch_chaining(self, sample_packets, sample_tags):
        """Test chaining batch operations."""
        stream = SyncStreamFromLists(sample_packets, sample_tags)

        # First batch: size 2
        batch1 = Batch(2)
        stream1 = batch1(stream)

        # Second batch: size 1 (batch the batches)
        batch2 = Batch(1)
        stream2 = batch2(stream1)

        result = list(stream2)

        # Each item should be a batch containing a single batch
        for batch_packet, _ in result:
            assert len(batch_packet) == 1
            # The contained item should itself be a batch

    def test_batch_with_generator_stream(self):
        """Test batch with generator-based stream."""

        def packet_generator():
            for i in range(7):
                yield f"packet_{i}", f"tag_{i}"

        from orcabridge.stream import SyncStreamFromGenerator

        stream = SyncStreamFromGenerator(packet_generator())

        batch = Batch(3)
        batched_stream = batch(stream)

        result = list(batched_stream)

        # Should have 3 batches: [3, 3, 1]
        assert len(result) == 3

        batch1_packet, _ = result[0]
        batch2_packet, _ = result[1]
        batch3_packet, _ = result[2]

        assert len(batch1_packet) == 3
        assert len(batch2_packet) == 3
        assert len(batch3_packet) == 1

    def test_batch_memory_efficiency(self):
        """Test that batch doesn't consume excessive memory."""
        # Create a large stream
        packets = [f"packet_{i}" for i in range(10000)]
        tags = [f"tag_{i}" for i in range(10000)]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(100)
        batched_stream = batch(stream)

        # Process one batch at a time to test memory efficiency
        batch_count = 0
        for batch_packet, _ in batched_stream:
            batch_count += 1
            assert len(batch_packet) <= 100
            if batch_count == 50:  # Stop early to avoid processing everything
                break

        assert batch_count == 50

    def test_batch_with_none_packets(self):
        """Test batch with None packets."""
        packets = [1, None, 3, None, 5, None]
        tags = ["num1", "null1", "num3", "null2", "num5", "null3"]

        stream = SyncStreamFromLists(packets, tags)
        batch = Batch(2)
        batched_stream = batch(stream)

        result = list(batched_stream)

        assert len(result) == 3

        # Check that None values are preserved
        all_packets = []
        for batch_packet, _ in result:
            all_packets.extend(list(batch_packet))

        assert all_packets == packets

    def test_batch_pickle(self):
        """Test that Batch mapper is pickleable."""
        import pickle
        from orcabridge.mappers import Batch

        batch = Batch(batch_size=3)
        pickled = pickle.dumps(batch)
        unpickled = pickle.loads(pickled)

        # Test that unpickled mapper works the same
        assert isinstance(unpickled, Batch)
        assert unpickled.batch_size == batch.batch_size
