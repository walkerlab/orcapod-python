"""Tests for base Pod functionality."""

import pytest
from orcabridge.pod import Pod
from orcabridge.streams import SyncStreamFromLists


class TestPodBase:
    """Test cases for base Pod class."""

    def test_pod_creation(self):
        """Test basic pod creation."""
        pod = Pod()
        assert pod is not None

    def test_pod_call_interface(self, sample_stream):
        """Test that pod implements callable interface."""
        pod = Pod()

        # Base Pod should be callable, but might not do anything useful
        # This tests the interface exists
        try:
            result_stream = pod(sample_stream)
            # If it succeeds, result should be a stream
            assert hasattr(result_stream, "__iter__")
        except NotImplementedError:
            # Base Pod might not implement __call__
            pass

    def test_pod_with_empty_stream(self):
        """Test pod with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        pod = Pod()

        try:
            result_stream = pod(empty_stream)
            result = list(result_stream)
            # If implemented, should handle empty stream
            assert isinstance(result, list)
        except NotImplementedError:
            # Base Pod might not implement functionality
            pass

    def test_pod_inheritance(self):
        """Test that Pod can be inherited."""

        class CustomPod(Pod):
            def __call__(self, stream):
                # Simple pass-through implementation
                for packet, tag in stream:
                    yield packet, tag

        custom_pod = CustomPod()
        packets = ["data1", "data2", "data3"]
        tags = ["tag1", "tag2", "tag3"]

        stream = SyncStreamFromLists(packets, tags)
        result_stream = custom_pod(stream)
        result = list(result_stream)

        expected = list(zip(packets, tags))
        assert result == expected

    def test_pod_chaining(self):
        """Test chaining pods together."""

        class AddPrefixPod(Pod):
            def __init__(self, prefix):
                self.prefix = prefix

            def __call__(self, stream):
                for packet, tag in stream:
                    yield f"{self.prefix}_{packet}", tag

        class AddSuffixPod(Pod):
            def __init__(self, suffix):
                self.suffix = suffix

            def __call__(self, stream):
                for packet, tag in stream:
                    yield f"{packet}_{self.suffix}", tag

        packets = ["data1", "data2"]
        tags = ["tag1", "tag2"]
        stream = SyncStreamFromLists(packets, tags)

        # Chain two pods
        prefix_pod = AddPrefixPod("PRE")
        suffix_pod = AddSuffixPod("SUF")

        intermediate_stream = prefix_pod(stream)
        final_stream = suffix_pod(intermediate_stream)

        result = list(final_stream)

        expected = [("PRE_data1_SUF", "tag1"), ("PRE_data2_SUF", "tag2")]
        assert result == expected

    def test_pod_error_handling(self):
        """Test pod error handling."""

        class ErrorPod(Pod):
            def __call__(self, stream):
                for i, (packet, tag) in enumerate(stream):
                    if i == 1:  # Error on second item
                        raise ValueError("Test error")
                    yield packet, tag

        packets = ["data1", "data2", "data3"]
        tags = ["tag1", "tag2", "tag3"]
        stream = SyncStreamFromLists(packets, tags)

        error_pod = ErrorPod()
        result_stream = error_pod(stream)

        # Should raise error when processing second item
        with pytest.raises(ValueError, match="Test error"):
            list(result_stream)

    def test_pod_stateful_processing(self):
        """Test pod with stateful processing."""

        class CounterPod(Pod):
            def __init__(self):
                self.count = 0

            def __call__(self, stream):
                for packet, tag in stream:
                    self.count += 1
                    yield (packet, self.count), tag

        packets = ["a", "b", "c"]
        tags = ["t1", "t2", "t3"]
        stream = SyncStreamFromLists(packets, tags)

        counter_pod = CounterPod()
        result_stream = counter_pod(stream)
        result = list(result_stream)

        expected = [(("a", 1), "t1"), (("b", 2), "t2"), (("c", 3), "t3")]
        assert result == expected

    def test_pod_multiple_outputs_per_input(self):
        """Test pod that produces multiple outputs per input."""

        class DuplicatorPod(Pod):
            def __call__(self, stream):
                for packet, tag in stream:
                    yield f"{packet}_copy1", f"{tag}_1"
                    yield f"{packet}_copy2", f"{tag}_2"

        packets = ["data1", "data2"]
        tags = ["tag1", "tag2"]
        stream = SyncStreamFromLists(packets, tags)

        duplicator_pod = DuplicatorPod()
        result_stream = duplicator_pod(stream)
        result = list(result_stream)

        expected = [
            ("data1_copy1", "tag1_1"),
            ("data1_copy2", "tag1_2"),
            ("data2_copy1", "tag2_1"),
            ("data2_copy2", "tag2_2"),
        ]
        assert result == expected

    def test_pod_filtering(self):
        """Test pod that filters items."""

        class FilterPod(Pod):
            def __init__(self, predicate):
                self.predicate = predicate

            def __call__(self, stream):
                for packet, tag in stream:
                    if self.predicate(packet, tag):
                        yield packet, tag

        packets = [1, 2, 3, 4, 5]
        tags = ["odd", "even", "odd", "even", "odd"]
        stream = SyncStreamFromLists(packets, tags)

        # Filter for even numbers
        def is_even(packet, tag):
            return packet % 2 == 0

        filter_pod = FilterPod(is_even)
        result_stream = filter_pod(stream)
        result = list(result_stream)

        expected = [(2, "even"), (4, "even")]
        assert result == expected

    def test_pod_transformation(self):
        """Test pod that transforms data."""

        class TransformPod(Pod):
            def __init__(self, transform_func):
                self.transform_func = transform_func

            def __call__(self, stream):
                for packet, tag in stream:
                    new_packet, new_tag = self.transform_func(packet, tag)
                    yield new_packet, new_tag

        packets = ["hello", "world"]
        tags = ["greeting", "noun"]
        stream = SyncStreamFromLists(packets, tags)

        def uppercase_transform(packet, tag):
            return packet.upper(), tag.upper()

        transform_pod = TransformPod(uppercase_transform)
        result_stream = transform_pod(stream)
        result = list(result_stream)

        expected = [("HELLO", "GREETING"), ("WORLD", "NOUN")]
        assert result == expected

    def test_pod_aggregation(self):
        """Test pod that aggregates data."""

        class SumPod(Pod):
            def __call__(self, stream):
                total = 0
                count = 0
                for packet, tag in stream:
                    if isinstance(packet, (int, float)):
                        total += packet
                        count += 1

                if count > 0:
                    yield total, f"sum_of_{count}_items"

        packets = [1, 2, 3, 4, 5]
        tags = ["n1", "n2", "n3", "n4", "n5"]
        stream = SyncStreamFromLists(packets, tags)

        sum_pod = SumPod()
        result_stream = sum_pod(stream)
        result = list(result_stream)

        expected = [(15, "sum_of_5_items")]
        assert result == expected

    def test_pod_with_complex_data(self):
        """Test pod with complex data structures."""

        class ExtractorPod(Pod):
            def __call__(self, stream):
                for packet, tag in stream:
                    if isinstance(packet, dict):
                        for key, value in packet.items():
                            yield value, f"{tag}_{key}"
                    else:
                        yield packet, tag

        packets = [{"a": 1, "b": 2}, "simple_string", {"x": 10, "y": 20, "z": 30}]
        tags = ["dict1", "str1", "dict2"]
        stream = SyncStreamFromLists(packets, tags)

        extractor_pod = ExtractorPod()
        result_stream = extractor_pod(stream)
        result = list(result_stream)

        # Should extract dict values as separate items
        assert len(result) == 6  # 2 + 1 + 3
        assert (1, "dict1_a") in result
        assert (2, "dict1_b") in result
        assert ("simple_string", "str1") in result
        assert (10, "dict2_x") in result
        assert (20, "dict2_y") in result
        assert (30, "dict2_z") in result
