"""
Test module for recursive features and advanced pipeline patterns.

This module tests advanced orcabridge features including recursive stream
operations, label chaining, length operations, source invocation patterns,
and complex pipeline compositions as demonstrated in the notebooks.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from orcabridge.base import SyncStream, Operation
from orcabridge.streams import SyncStreamFromLists, SyncStreamFromGenerator
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
    CacheStream,
)
from orcabridge.sources import GlobSource
from orcabridge.pod import FunctionPod


@pytest.fixture
def hierarchical_data():
    """Hierarchical data for testing recursive operations."""
    return [
        (
            {"level": 1, "parent": None, "id": "root"},
            {"name": "Root", "children": ["a", "b"]},
        ),
        (
            {"level": 2, "parent": "root", "id": "a"},
            {"name": "Node A", "children": ["a1", "a2"]},
        ),
        (
            {"level": 2, "parent": "root", "id": "b"},
            {"name": "Node B", "children": ["b1"]},
        ),
        ({"level": 3, "parent": "a", "id": "a1"}, {"name": "Leaf A1", "children": []}),
        ({"level": 3, "parent": "a", "id": "a2"}, {"name": "Leaf A2", "children": []}),
        ({"level": 3, "parent": "b", "id": "b1"}, {"name": "Leaf B1", "children": []}),
    ]


@pytest.fixture
def temp_nested_files():
    """Create nested file structure for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create nested directory structure
        (temp_path / "level1").mkdir()
        (temp_path / "level1" / "level2").mkdir()

        files = {}

        # Root level files
        for i in range(3):
            file_path = temp_path / f"root_{i}.txt"
            with open(file_path, "w") as f:
                f.write(f"Root file {i}")
            files[f"root_{i}"] = file_path

        # Level 1 files
        for i in range(2):
            file_path = temp_path / "level1" / f"l1_{i}.txt"
            with open(file_path, "w") as f:
                f.write(f"Level 1 file {i}")
            files[f"l1_{i}"] = file_path

        # Level 2 files
        file_path = temp_path / "level1" / "level2" / "l2_0.txt"
        with open(file_path, "w") as f:
            f.write("Level 2 file")
        files["l2_0"] = file_path

        yield temp_path, files


class TestRecursiveStreamOperations:
    """Test recursive and self-referential stream operations."""

    def test_recursive_stream_processing(self, hierarchical_data):
        """Test recursive processing of hierarchical data."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        def process_level(stream, max_level=3):
            """Recursively process each level."""

            def level_processor(tag, packet):
                level = tag["level"]
                if level < max_level:
                    # Add processing marker
                    return tag, {**packet, f"processed_level_{level}": True}
                else:
                    # Leaf nodes get different processing
                    return tag, {**packet, "is_leaf": True}

            return Transform(level_processor)(stream)

        # Apply recursive processing
        processed = process_level(source_stream)
        result = list(processed)

        # Check that different levels are processed differently
        for tag, packet in result:
            level = tag["level"]
            if level < 3:
                assert f"processed_level_{level}" in packet
            else:
                assert packet["is_leaf"] is True

    def test_recursive_stream_expansion(self, hierarchical_data):
        """Test recursive expansion of stream data."""
        # Start with root nodes only
        root_data = [item for item in hierarchical_data if item[0]["parent"] is None]
        tags, packets = zip(*root_data)
        root_stream = SyncStreamFromLists(list(tags), list(packets))

        def expand_children(tag, packet):
            """Generate child nodes for each parent."""
            children = packet.get("children", [])
            for child_id in children:
                # Find child data from hierarchical_data
                for h_tag, h_packet in hierarchical_data:
                    if h_tag["id"] == child_id:
                        yield h_tag, h_packet
                        break

        # Create expanding pod
        expander = FunctionPod(expand_children)
        expanded = expander(root_stream)
        result = list(expanded)

        # Should have expanded to include all children
        assert len(result) >= 2  # At least the immediate children

    def test_recursive_filtering_cascade(self, hierarchical_data):
        """Test recursive filtering that cascades through levels."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create a cascade of filters for each level
        level_1_filter = Filter(lambda tag, packet: tag["level"] == 1)
        level_2_filter = Filter(lambda tag, packet: tag["level"] <= 2)
        level_3_filter = Filter(lambda tag, packet: tag["level"] <= 3)

        # Apply filters recursively
        def recursive_filter(stream, current_level=1):
            if current_level == 1:
                filtered = level_1_filter(stream)
            elif current_level == 2:
                filtered = level_2_filter(stream)
            else:
                filtered = level_3_filter(stream)

            return filtered

        # Test each level
        level_1_result = list(recursive_filter(source_stream, 1))
        level_2_result = list(recursive_filter(source_stream, 2))
        level_3_result = list(recursive_filter(source_stream, 3))

        assert len(level_1_result) == 1  # Only root
        assert len(level_2_result) == 3  # Root + level 2 nodes
        assert len(level_3_result) == 6  # All nodes

    def test_self_referential_stream_operations(self, hierarchical_data):
        """Test operations that reference the stream itself."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Cache the stream for self-reference
        cache = CacheStream()
        cached_stream = cache(source_stream)

        # Consume the cache
        list(cached_stream)

        # Now create operations that reference the cached data
        def find_parent_info(tag, packet):
            parent_id = tag.get("parent")
            if parent_id:
                # Look up parent in cached stream
                for cached_tag, cached_packet in cache.cache:
                    if cached_tag["id"] == parent_id:
                        return tag, {
                            **packet,
                            "parent_name": cached_packet["name"],
                            "parent_level": cached_tag["level"],
                        }
            return tag, {**packet, "parent_name": None, "parent_level": None}

        # Apply parent lookup
        enriched = Transform(find_parent_info)(cached_stream)
        result = list(enriched)

        # Check parent information was added
        for tag, packet in result:
            if tag["parent"] is not None:
                assert packet["parent_name"] is not None
                assert packet["parent_level"] is not None


class TestLabelAndLengthOperations:
    """Test label manipulation and length operations."""

    def test_label_chaining_operations(self, hierarchical_data):
        """Test chaining operations with label tracking."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(
            list(tags), list(packets), label="hierarchical_source"
        )

        # Create labeled operations
        filter_op = Filter(lambda tag, packet: tag["level"] <= 2)
        transform_op = Transform(
            lambda tag, packet: (tag, {**packet, "processed": True})
        )

        # Apply operations and track labels
        filtered = filter_op(source_stream)
        assert filtered.label.startswith("Filter_")

        transformed = transform_op(filtered)
        assert transformed.label.startswith("Transform_")

        # Check that invocation chain is maintained
        result = list(transformed)
        assert len(result) == 3  # Root + 2 level-2 nodes

    def test_stream_length_operations(self, hierarchical_data):
        """Test operations that depend on stream length."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        def length_dependent_transform(tag, packet):
            # This would need to know stream length
            # For simulation, we'll use a mock length
            stream_length = 6  # Known length of hierarchical_data
            return tag, {
                **packet,
                "relative_position": tag["level"] / 3,  # Relative to max level
                "is_majority_level": tag["level"] == 3,  # Most nodes are level 3
            }

        processed = Transform(length_dependent_transform)(source_stream)
        result = list(processed)

        # Check length-dependent calculations
        for tag, packet in result:
            assert "relative_position" in packet
            assert "is_majority_level" in packet
            if tag["level"] == 3:
                assert packet["is_majority_level"] is True

    def test_dynamic_label_generation(self, hierarchical_data):
        """Test dynamic label generation based on stream content."""
        tags, packets = zip(*hierarchical_data)

        # Create streams with content-based labels
        def create_labeled_stream(data, label_func):
            stream_tags, stream_packets = zip(*data)
            label = label_func(data)
            return SyncStreamFromLists(
                list(stream_tags), list(stream_packets), label=label
            )

        # Different labeling strategies
        level_1_data = [item for item in hierarchical_data if item[0]["level"] == 1]
        level_2_data = [item for item in hierarchical_data if item[0]["level"] == 2]
        level_3_data = [item for item in hierarchical_data if item[0]["level"] == 3]

        stream_1 = create_labeled_stream(
            level_1_data, lambda data: f"level_1_stream_{len(data)}_items"
        )
        stream_2 = create_labeled_stream(
            level_2_data, lambda data: f"level_2_stream_{len(data)}_items"
        )
        stream_3 = create_labeled_stream(
            level_3_data, lambda data: f"level_3_stream_{len(data)}_items"
        )

        assert stream_1.label == "level_1_stream_1_items"
        assert stream_2.label == "level_2_stream_2_items"
        assert stream_3.label == "level_3_stream_3_items"


class TestSourceInvocationPatterns:
    """Test advanced source invocation and composition patterns."""

    def test_multiple_source_composition(self, temp_nested_files):
        """Test composing multiple sources with different patterns."""
        temp_path, files = temp_nested_files

        # Create different sources for different levels
        root_source = GlobSource(str(temp_path / "*.txt"), label="root_files")
        level1_source = GlobSource(
            str(temp_path / "level1" / "*.txt"), label="level1_files"
        )
        level2_source = GlobSource(
            str(temp_path / "level1" / "level2" / "*.txt"), label="level2_files"
        )

        # Compose sources
        all_sources = Merge()(root_source, level1_source, level2_source)
        result = list(all_sources)

        # Should have files from all levels
        assert len(result) >= 6  # 3 root + 2 level1 + 1 level2

        # Check that files from different levels are included
        paths = [tag["path"] for tag, packet in result]
        assert any("root_" in str(path) for path in paths)
        assert any("l1_" in str(path) for path in paths)
        assert any("l2_" in str(path) for path in paths)

    def test_conditional_source_invocation(self, temp_nested_files):
        """Test conditional source invocation based on data content."""
        temp_path, files = temp_nested_files

        def conditional_source_factory(condition):
            """Create source based on condition."""
            if condition == "root":
                return GlobSource(str(temp_path / "*.txt"))
            elif condition == "nested":
                return GlobSource(str(temp_path / "**" / "*.txt"))
            else:
                return SyncStreamFromLists([], [])  # Empty stream

        # Test different conditions
        root_stream = conditional_source_factory("root")
        nested_stream = conditional_source_factory("nested")
        empty_stream = conditional_source_factory("other")

        root_result = list(root_stream)
        nested_result = list(nested_stream)
        empty_result = list(empty_stream)

        assert len(root_result) == 3  # Only root files
        assert len(nested_result) >= 6  # All files recursively
        assert len(empty_result) == 0

    def test_recursive_source_generation(self, temp_nested_files):
        """Test recursive generation of sources."""
        temp_path, files = temp_nested_files

        def recursive_file_processor(tag, packet):
            """Process file and potentially generate more sources."""
            file_path = Path(tag["path"])

            # If this is a directory-like file, yield info about subdirectories
            if "level1" in str(file_path.parent):
                # This file is in level1, so it knows about level2
                yield tag, {**packet, "has_subdirs": True, "subdir_count": 1}
            else:
                yield tag, {**packet, "has_subdirs": False, "subdir_count": 0}

        # Start with root source
        root_source = GlobSource(str(temp_path / "*.txt"))

        # Apply recursive processing
        processor = FunctionPod(recursive_file_processor)
        processed = processor(root_source)
        result = list(processed)

        # Check recursive information
        for tag, packet in result:
            assert "has_subdirs" in packet
            assert "subdir_count" in packet

    def test_source_caching_and_reuse(self, temp_nested_files):
        """Test caching and reusing source results."""
        temp_path, files = temp_nested_files

        # Create cached source
        source = GlobSource(str(temp_path / "*.txt"))
        cache = CacheStream()
        cached_source = cache(source)

        # First consumption
        result1 = list(cached_source)

        # Verify caching worked
        assert cache.is_cached
        assert len(cache.cache) == 3

        # Create new operations using cached source
        filter_op = Filter(lambda tag, packet: "root_1" in str(tag["path"]))
        transform_op = Transform(lambda tag, packet: (tag, {**packet, "reused": True}))

        # Apply operations to cached source
        filtered = filter_op(cache())  # Use cached version
        transformed = transform_op(cache())  # Use cached version again

        filter_result = list(filtered)
        transform_result = list(transformed)

        # Both should work independently using cached data
        assert len(filter_result) == 1  # Only root_1 file
        assert len(transform_result) == 3  # All files with reused flag

        for tag, packet in transform_result:
            assert packet["reused"] is True


class TestComplexPipelinePatterns:
    """Test complex pipeline patterns and compositions."""

    def test_branching_and_merging_pipeline(self, hierarchical_data):
        """Test pipeline that branches and merges back together."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create branches for different processing paths
        branch_a = (
            source_stream
            >> Filter(lambda tag, packet: tag["level"] <= 2)
            >> Transform(
                lambda tag, packet: (tag, {**packet, "branch": "A", "priority": "high"})
            )
        )

        branch_b = (
            source_stream
            >> Filter(lambda tag, packet: tag["level"] == 3)
            >> Transform(
                lambda tag, packet: (tag, {**packet, "branch": "B", "priority": "low"})
            )
        )

        # Merge branches back together
        merged = Merge()(branch_a, branch_b)
        result = list(merged)

        # Should have all original items but with branch processing
        assert len(result) == 6

        # Check branch assignments
        branches = [packet["branch"] for tag, packet in result]
        assert "A" in branches
        assert "B" in branches

    def test_multi_level_pipeline_composition(self, hierarchical_data):
        """Test multi-level pipeline composition with nested operations."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Level 1: Basic filtering and transformation
        level1_pipeline = (
            source_stream
            >> Filter(lambda tag, packet: len(packet["name"]) > 5)
            >> Transform(
                lambda tag, packet: (tag, {**packet, "level1_processed": True})
            )
        )

        # Level 2: Advanced processing based on level 1
        level2_pipeline = (
            level1_pipeline
            >> MapTags({"level": "hierarchy_level", "id": "node_id"})
            >> MapPackets({"name": "node_name", "children": "child_nodes"})
        )

        # Level 3: Final aggregation and summary
        level3_pipeline = level2_pipeline >> Transform(
            lambda tag, packet: (
                tag,
                {
                    **packet,
                    "final_processed": True,
                    "child_count": len(packet["child_nodes"]),
                    "has_children": len(packet["child_nodes"]) > 0,
                },
            )
        )

        result = list(level3_pipeline)

        # Check multi-level processing
        for tag, packet in result:
            assert packet["level1_processed"] is True
            assert packet["final_processed"] is True
            assert "hierarchy_level" in tag
            assert "node_name" in packet
            assert "child_count" in packet

    def test_pipeline_with_feedback_loop(self, hierarchical_data):
        """Test pipeline pattern that simulates feedback loops."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        # Create a cache to simulate feedback
        feedback_cache = CacheStream()

        # First pass: process and cache
        first_pass = (
            source_stream
            >> Transform(lambda tag, packet: (tag, {**packet, "pass": 1}))
            >> feedback_cache
        )

        # Consume first pass to populate cache
        first_result = list(first_pass)

        # Second pass: use cached data for enrichment
        def enrich_with_feedback(tag, packet):
            # Use cached data to enrich current item
            related_items = []
            for cached_tag, cached_packet in feedback_cache.cache:
                if (
                    cached_tag["level"] == tag["level"]
                    and cached_tag["id"] != tag["id"]
                ):
                    related_items.append(cached_packet["name"])

            return tag, {
                **packet,
                "pass": 2,
                "related_items": related_items,
                "relation_count": len(related_items),
            }

        second_pass = Transform(enrich_with_feedback)(feedback_cache())
        second_result = list(second_pass)

        # Check feedback enrichment
        for tag, packet in second_result:
            assert packet["pass"] == 2
            assert "related_items" in packet
            assert "relation_count" in packet

    def test_pipeline_error_handling_and_recovery(self, hierarchical_data):
        """Test pipeline error handling and recovery patterns."""
        tags, packets = zip(*hierarchical_data)
        source_stream = SyncStreamFromLists(list(tags), list(packets))

        def potentially_failing_operation(tag, packet):
            # Fail on specific condition
            if tag["id"] == "a1":  # Fail on specific node
                raise ValueError("Processing failed for a1")
            return tag, {**packet, "processed": True}

        # Create error-tolerant pipeline
        def error_tolerant_transform(tag, packet):
            try:
                return potentially_failing_operation(tag, packet)
            except ValueError:
                # Recovery: mark as failed but continue
                return tag, {**packet, "processed": False, "error": True}

        pipeline = Transform(error_tolerant_transform)(source_stream)
        result = list(pipeline)

        # Should have processed all items despite error
        assert len(result) == 6

        # Check error handling
        failed_items = [
            item for tag, packet in result for item in [packet] if packet.get("error")
        ]
        successful_items = [
            item
            for tag, packet in result
            for item in [packet]
            if packet.get("processed")
        ]

        assert len(failed_items) == 1  # One failed item
        assert len(successful_items) == 5  # Five successful items

    def test_dynamic_pipeline_construction(self, hierarchical_data):
        """Test dynamic construction of pipelines based on data characteristics."""
        tags, packets = zip(*hierarchical_data)

        def build_dynamic_pipeline(data):
            """Build pipeline based on data characteristics."""
            # Analyze data
            levels = set(tag["level"] for tag, packet in data)
            max_level = max(levels)
            has_children = any(len(packet["children"]) > 0 for tag, packet in data)

            # Build pipeline dynamically
            base_stream = SyncStreamFromLists(
                [tag for tag, packet in data], [packet for tag, packet in data]
            )

            operations = [base_stream]

            # Add level-specific processing
            if max_level > 2:
                operations.append(
                    Transform(
                        lambda tag, packet: (tag, {**packet, "is_deep_hierarchy": True})
                    )
                )

            # Add child processing if needed
            if has_children:
                operations.append(
                    Transform(
                        lambda tag, packet: (
                            tag,
                            {
                                **packet,
                                "child_info": f"has_{len(packet['children'])}_children",
                            },
                        )
                    )
                )

            # Chain operations
            pipeline = operations[0]
            for op in operations[1:]:
                if isinstance(op, Transform):
                    pipeline = op(pipeline)

            return pipeline

        # Build and execute dynamic pipeline
        dynamic_pipeline = build_dynamic_pipeline(hierarchical_data)
        result = list(dynamic_pipeline)

        # Check dynamic processing
        for tag, packet in result:
            assert "is_deep_hierarchy" in packet  # Should be added due to max_level > 2
            assert "child_info" in packet  # Should be added due to has_children
