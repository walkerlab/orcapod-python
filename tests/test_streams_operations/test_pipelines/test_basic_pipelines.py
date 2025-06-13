"""
Test module for basic pipeline operations.

This module tests fundamental pipeline construction and execution,
including chaining operations, combining multiple streams, and
basic data flow patterns as demonstrated in the notebooks.
"""

import pytest
import tempfile
from pathlib import Path

from orcabridge.base import SyncStream
from orcabridge.streams import SyncStreamFromLists
from orcabridge.mappers import (
    Join,
    Merge,
    Filter,
    Transform,
    MapPackets,
    MapTags,
    Repeat,
    DefaultTag,
    Batch,
    FirstMatch,
)
from orcabridge.sources import GlobSource
from orcabridge.pod import FunctionPod


@pytest.fixture
def temp_files():
    """Create temporary files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create test files
        files = {}
        for i in range(1, 4):
            file_path = temp_path / f"test_{i}.txt"
            content = f"Content of file {i}\nLine 2 of file {i}"
            with open(file_path, "w") as f:
                f.write(content)
            files[f"test_{i}.txt"] = file_path

        yield temp_path, files


@pytest.fixture
def sample_user_data():
    """Sample user data for pipeline testing."""
    return [
        ({"user_id": 1, "session": "a"}, {"name": "Alice", "age": 25, "score": 85}),
        ({"user_id": 2, "session": "a"}, {"name": "Bob", "age": 30, "score": 92}),
        ({"user_id": 3, "session": "b"}, {"name": "Charlie", "age": 28, "score": 78}),
        ({"user_id": 1, "session": "b"}, {"name": "Alice", "age": 25, "score": 88}),
    ]


@pytest.fixture
def sample_metadata():
    """Sample metadata for joining."""
    return [
        ({"user_id": 1}, {"department": "Engineering", "level": "Senior"}),
        ({"user_id": 2}, {"department": "Marketing", "level": "Junior"}),
        ({"user_id": 3}, {"department": "Engineering", "level": "Mid"}),
    ]


class TestBasicPipelineConstruction:
    """Test basic pipeline construction patterns."""

    def test_simple_linear_pipeline(self, sample_user_data):
        """Test simple linear pipeline with chained operations."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Build pipeline: filter -> transform -> map packets
        pipeline = (
            source_stream
            >> Filter(lambda tag, packet: packet["age"] >= 28)
            >> Transform(
                lambda tag, packet: (tag, {**packet, "category": "experienced"})
            )
            >> MapPackets({"name": "full_name", "score": "performance"})
        )

        result = list(pipeline)

        # Should have filtered out users under 28
        assert len(result) == 3

        # Check transformations applied
        for tag, packet in result:
            assert packet["age"] >= 28
            assert packet["category"] == "experienced"
            assert "full_name" in packet
            assert "performance" in packet
            assert "name" not in packet  # Should be mapped
            assert "score" not in packet  # Should be mapped

    def test_pipeline_with_join(self, sample_user_data, sample_metadata):
        """Test pipeline with join operation."""
        # Create streams
        user_tags, user_packets = zip(*sample_user_data)
        meta_tags, meta_packets = zip(*sample_metadata)

        user_stream = SyncStreamFromLists(list(user_tags), list(user_packets))
        meta_stream = SyncStreamFromLists(list(meta_tags), list(meta_packets))

        # Join streams on user_id
        joined = Join()(user_stream, meta_stream)
        result = list(joined)

        # Should have joined records where user_id matches
        assert len(result) >= 2  # At least Alice and Bob should match

        # Check that joined data has both user and metadata info
        for tag, packet in result:
            assert "user_id" in tag
            assert "name" in packet  # From user data
            assert "department" in packet  # From metadata

    def test_pipeline_with_merge(self, sample_user_data):
        """Test pipeline with merge operation."""
        tags, packets = zip(*sample_user_data)

        # Split data into two streams
        stream1 = SyncStreamFromLists(list(tags[:2]), list(packets[:2]))
        stream2 = SyncStreamFromLists(list(tags[2:]), list(packets[2:]))

        # Merge streams
        merged = Merge()(stream1, stream2)
        result = list(merged)

        # Should have all items from both streams
        assert len(result) == 4

        # Order might be different but all data should be present
        result_user_ids = [tag["user_id"] for tag, packet in result]
        expected_user_ids = [tag["user_id"] for tag, packet in sample_user_data]
        assert sorted(result_user_ids) == sorted(expected_user_ids)

    def test_pipeline_with_batch_processing(self, sample_user_data):
        """Test pipeline with batch processing."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create batches of size 2
        batched = Batch(batch_size=2)(source_stream)
        result = list(batched)

        # Should have 2 batches (4 items / 2 per batch)
        assert len(result) == 2

        # Each result should be a batch
        for tag, packet in result:
            assert isinstance(packet, list)
            assert len(packet) == 2
            # Tag should be batch representation of individual tags
            assert isinstance(tag, dict)

    def test_pipeline_with_repeat_operation(self, sample_user_data):
        """Test pipeline with repeat operation."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(
            list(tags[:2]), list(packets[:2])
        )  # Use first 2 items

        # Repeat each item 3 times
        repeated = Repeat(repeat_count=3)(source_stream)
        result = list(repeated)

        # Should have 6 items total (2 original * 3 repeats)
        assert len(result) == 6

        # Check that items are correctly repeated
        assert result[0] == result[1] == result[2]  # First item repeated
        assert result[3] == result[4] == result[5]  # Second item repeated

    def test_complex_multi_stage_pipeline(self, sample_user_data, sample_metadata):
        """Test complex pipeline with multiple stages and branches."""
        # Create source streams
        user_tags, user_packets = zip(*sample_user_data)
        meta_tags, meta_packets = zip(*sample_metadata)

        user_stream = SyncStreamFromLists(list(user_tags), list(user_packets))
        meta_stream = SyncStreamFromLists(list(meta_tags), list(meta_packets))

        # Complex pipeline:
        # 1. Add default tags to user stream
        # 2. Join with metadata
        # 3. Filter by age and score
        # 4. Transform and map fields
        pipeline = (
            DefaultTag({"source": "user_system"})(user_stream)
            * meta_stream  # Join operation
            >> Filter(lambda tag, packet: packet["age"] >= 25 and packet["score"] >= 80)
            >> Transform(
                lambda tag, packet: (
                    {**tag, "processed": True},
                    {**packet, "grade": "A" if packet["score"] >= 90 else "B"},
                )
            )
            >> MapPackets({"name": "employee_name", "department": "dept"})
        )

        result = list(pipeline)

        # Verify complex transformations
        for tag, packet in result:
            assert tag["source"] == "user_system"
            assert tag["processed"] is True
            assert packet["age"] >= 25
            assert packet["score"] >= 80
            assert packet["grade"] in ["A", "B"]
            assert "employee_name" in packet
            assert "dept" in packet

    def test_pipeline_error_propagation(self, sample_user_data):
        """Test that errors propagate correctly through pipeline."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create pipeline with operation that will fail
        def failing_transform(tag, packet):
            if packet["age"] > 29:
                raise ValueError("Age too high!")
            return tag, packet

        pipeline = source_stream >> Transform(failing_transform)

        # Should propagate the error
        with pytest.raises(ValueError, match="Age too high!"):
            list(pipeline)

    def test_pipeline_with_empty_stream(self):
        """Test pipeline behavior with empty streams."""
        empty_stream = SyncStreamFromLists([], [])

        # Apply operations to empty stream
        pipeline = (
            empty_stream
            >> Filter(lambda tag, packet: True)
            >> Transform(lambda tag, packet: (tag, {**packet, "processed": True}))
        )

        result = list(pipeline)
        assert result == []

    def test_pipeline_with_first_match(self, sample_user_data, sample_metadata):
        """Test pipeline with FirstMatch operation."""
        user_tags, user_packets = zip(*sample_user_data)
        meta_tags, meta_packets = zip(*sample_metadata)

        user_stream = SyncStreamFromLists(list(user_tags), list(user_packets))
        meta_stream = SyncStreamFromLists(list(meta_tags), list(meta_packets))

        # Use FirstMatch instead of Join
        matched = FirstMatch()(user_stream, meta_stream)
        result = list(matched)

        # FirstMatch should consume items from both streams
        assert len(result) <= len(sample_user_data)

        # Each result should have matched data
        for tag, packet in result:
            assert "user_id" in tag
            assert "name" in packet or "department" in packet


class TestPipelineDataFlow:
    """Test data flow patterns in pipelines."""

    def test_data_preservation_through_pipeline(self, sample_user_data):
        """Test that data is correctly preserved through transformations."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Track original data
        original_user_ids = [tag["user_id"] for tag, packet in sample_user_data]
        original_names = [packet["name"] for tag, packet in sample_user_data]

        # Pipeline that shouldn't lose data
        pipeline = (
            source_stream
            >> MapTags({"user_id": "id"})  # Rename tag field
            >> MapPackets({"name": "username"})  # Rename packet field
        )

        result = list(pipeline)

        # Check data preservation
        result_ids = [tag["id"] for tag, packet in result]
        result_names = [packet["username"] for tag, packet in result]

        assert sorted(result_ids) == sorted(original_user_ids)
        assert sorted(result_names) == sorted(original_names)

    def test_data_aggregation_pipeline(self, sample_user_data):
        """Test pipeline that aggregates data."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Aggregate by session
        def aggregate_by_session(tag, packet):
            return {"session": tag["session"]}, {
                "users": [packet["name"]],
                "avg_score": packet["score"],
                "count": 1,
            }

        # Transform and then batch by session (simplified aggregation)
        pipeline = source_stream >> Transform(aggregate_by_session)

        result = list(pipeline)

        # Should have transformed all items
        assert len(result) == len(sample_user_data)

        # Check session-based grouping
        sessions = [tag["session"] for tag, packet in result]
        assert "a" in sessions
        assert "b" in sessions

    def test_conditional_processing_pipeline(self, sample_user_data):
        """Test pipeline with conditional processing branches."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Split into high and low performers
        high_performers = (
            source_stream
            >> Filter(lambda tag, packet: packet["score"] >= 85)
            >> Transform(
                lambda tag, packet: (
                    {**tag, "category": "high"},
                    {**packet, "bonus": packet["score"] * 0.1},
                )
            )
        )

        low_performers = (
            source_stream
            >> Filter(lambda tag, packet: packet["score"] < 85)
            >> Transform(
                lambda tag, packet: (
                    {**tag, "category": "low"},
                    {**packet, "training": True},
                )
            )
        )

        # Merge results
        combined = Merge()(high_performers, low_performers)
        result = list(combined)

        # Check that all items are categorized
        categories = [tag["category"] for tag, packet in result]
        assert "high" in categories
        assert "low" in categories

        # Check conditional processing
        for tag, packet in result:
            if tag["category"] == "high":
                assert "bonus" in packet
                assert packet["score"] >= 85
            else:
                assert "training" in packet
                assert packet["score"] < 85


class TestPipelineWithSources:
    """Test pipelines starting from sources."""

    def test_pipeline_from_glob_source(self, temp_files):
        """Test pipeline starting from GlobSource."""
        temp_dir, files = temp_files

        # Create source
        source = GlobSource(str(temp_dir / "*.txt"))

        # Build pipeline
        pipeline = (
            source
            >> Transform(
                lambda tag, packet: (
                    {**tag, "processed": True},
                    {**packet, "line_count": len(packet["content"].split("\n"))},
                )
            )
            >> Filter(lambda tag, packet: packet["line_count"] >= 2)
        )

        result = list(pipeline)

        # Should have all files (each has 2 lines)
        assert len(result) == 3

        # Check processing
        for tag, packet in result:
            assert tag["processed"] is True
            assert packet["line_count"] == 2
            assert "path" in tag

    def test_pipeline_with_function_pod(self, sample_user_data):
        """Test pipeline with FunctionPod processing."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create processing function
        def enrich_user_data(tag, packet):
            """Add computed fields to user data."""
            return tag, {
                **packet,
                "age_group": "young" if packet["age"] < 30 else "mature",
                "performance": "excellent" if packet["score"] >= 90 else "good",
            }

        # Create pod
        processor = FunctionPod(enrich_user_data)

        # Build pipeline
        pipeline = (
            source_stream
            >> processor
            >> Filter(lambda tag, packet: packet["performance"] == "excellent")
        )

        result = list(pipeline)

        # Check processing
        for tag, packet in result:
            assert packet["performance"] == "excellent"
            assert packet["age_group"] in ["young", "mature"]
            assert packet["score"] >= 90


class TestPipelineOptimization:
    """Test pipeline optimization and efficiency."""

    def test_pipeline_lazy_evaluation(self, sample_user_data):
        """Test that pipeline operations are lazily evaluated."""
        call_log = []

        def logging_transform(tag, packet):
            call_log.append(f"processing_{tag['user_id']}")
            return tag, packet

        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Build pipeline but don't execute
        pipeline = (
            source_stream
            >> Transform(logging_transform)
            >> Filter(lambda tag, packet: packet["age"] >= 28)
        )

        # No processing should have happened yet
        assert call_log == []

        # Start consuming pipeline
        iterator = iter(pipeline)
        next(iterator)

        # Now some processing should have happened
        assert len(call_log) >= 1

    def test_pipeline_memory_efficiency(self):
        """Test pipeline memory efficiency with large data."""

        def large_data_generator():
            for i in range(1000):
                yield ({"id": i}, {"value": i * 2, "data": f"item_{i}"})

        # Create pipeline that processes large stream
        from orcabridge.stream import SyncStreamFromGenerator

        source = SyncStreamFromGenerator(large_data_generator)
        pipeline = (
            source
            >> Filter(lambda tag, packet: tag["id"] % 10 == 0)  # Keep every 10th item
            >> Transform(lambda tag, packet: (tag, {**packet, "filtered": True}))
        )

        # Process in chunks
        count = 0
        for tag, packet in pipeline:
            assert packet["filtered"] is True
            assert tag["id"] % 10 == 0
            count += 1

            if count >= 10:  # Don't process all items
                break

        assert count == 10

    def test_pipeline_error_recovery(self, sample_user_data):
        """Test pipeline behavior with partial errors."""
        tags, packets = zip(*sample_user_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        def sometimes_failing_transform(tag, packet):
            if packet["name"] == "Bob":  # Fail for Bob
                raise ValueError("Bob processing failed")
            return tag, {**packet, "processed": True}

        # This pipeline will fail partway through
        pipeline = source_stream >> Transform(sometimes_failing_transform)

        # Should fail when reaching Bob
        with pytest.raises(ValueError, match="Bob processing failed"):
            list(pipeline)

    def test_pipeline_reusability(self, sample_user_data):
        """Test that pipeline components can be reused."""
        # Create reusable operations
        age_filter = Filter(lambda tag, packet: packet["age"] >= 28)
        score_transform = Transform(
            lambda tag, packet: (
                tag,
                {**packet, "grade": "A" if packet["score"] >= 90 else "B"},
            )
        )

        tags, packets = zip(*sample_user_data)
        stream1 = SyncStreamFromLists(list(tags[:2]), list(packets[:2]))
        stream2 = SyncStreamFromLists(list(tags[2:]), list(packets[2:]))

        # Apply same operations to different streams
        pipeline1 = stream1 >> age_filter >> score_transform
        pipeline2 = stream2 >> age_filter >> score_transform

        result1 = list(pipeline1)
        result2 = list(pipeline2)

        # Both should work independently
        for tag, packet in result1 + result2:
            if len([tag, packet]) > 0:  # If any results
                assert packet["age"] >= 28
                assert packet["grade"] in ["A", "B"]
