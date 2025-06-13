"""Tests for FunctionPod functionality."""

import pytest
from orcabridge.pod import FunctionPod
from orcabridge.streams import SyncStreamFromLists


class TestFunctionPod:
    """Test cases for FunctionPod."""

    def test_function_pod_no_output(self, sample_stream, func_no_output):
        """Test function pod with function that has no output."""
        pod = FunctionPod(func_no_output)
        result_stream = pod(sample_stream)

        result = list(result_stream)

        # Should produce no output
        assert len(result) == 0

    def test_function_pod_single_output(self, sample_stream, func_single_output):
        """Test function pod with function that has single output."""
        pod = FunctionPod(func_single_output)
        result_stream = pod(sample_stream)

        result = list(result_stream)

        # Should produce one output per input
        original_packets = list(sample_stream)
        assert len(result) == len(original_packets)

        for i, (packet, tag) in enumerate(result):
            expected_packet = f"processed_{original_packets[i][0]}"
            assert packet == expected_packet

    def test_function_pod_multiple_outputs(self, sample_stream, func_multiple_outputs):
        """Test function pod with function that has multiple outputs."""
        pod = FunctionPod(func_multiple_outputs)
        result_stream = pod(sample_stream)

        result = list(result_stream)

        # Should produce two outputs per input
        original_packets = list(sample_stream)
        assert len(result) == len(original_packets) * 2

        # Check that we get pairs of outputs
        for i in range(0, len(result), 2):
            original_idx = i // 2
            original_packet = original_packets[original_idx][0]

            # First output should be the packet itself
            assert result[i][0] == original_packet
            # Second output should be uppercased
            assert result[i + 1][0] == str(original_packet).upper()

    def test_function_pod_error_function(self, sample_stream, func_with_error):
        """Test function pod with function that raises error."""
        pod = FunctionPod(func_with_error)
        result_stream = pod(sample_stream)

        # Should raise error when processing
        with pytest.raises(ValueError, match="Function error"):
            list(result_stream)

    def test_function_pod_with_datastore(self, func_single_output, data_store):
        """Test function pod with datastore integration."""

        # Create a function that uses the datastore
        def datastore_function(inputs, datastore):
            packet, tag = inputs[0]
            # Store and retrieve from datastore
            datastore["processed_count"] = datastore.get("processed_count", 0) + 1
            return f"item_{datastore['processed_count']}_{packet}"

        pod = FunctionPod(datastore_function, datastore=data_store)

        packets = ["a", "b", "c"]
        tags = ["tag1", "tag2", "tag3"]
        stream = SyncStreamFromLists(packets, tags)

        result_stream = pod(stream)
        result = list(result_stream)

        # Should use datastore to track processing
        expected = [("item_1_a", "tag1"), ("item_2_b", "tag2"), ("item_3_c", "tag3")]
        assert result == expected
        assert data_store["processed_count"] == 3

    def test_function_pod_different_input_counts(self):
        """Test function pod with functions expecting different input counts."""

        # Function expecting 1 input
        def single_input_func(inputs):
            packet, tag = inputs[0]
            return f"single_{packet}"

        # Function expecting 2 inputs
        def double_input_func(inputs):
            if len(inputs) < 2:
                return None  # Not enough inputs
            packet1, tag1 = inputs[0]
            packet2, tag2 = inputs[1]
            return f"combined_{packet1}_{packet2}"

        packets = ["a", "b", "c", "d"]
        tags = ["t1", "t2", "t3", "t4"]
        stream = SyncStreamFromLists(packets, tags)

        # Test single input function
        pod1 = FunctionPod(single_input_func)
        result1 = list(pod1(stream))

        assert len(result1) == 4
        assert result1[0][0] == "single_a"
        assert result1[1][0] == "single_b"

        # Test double input function (if supported)
        # This behavior depends on FunctionPod implementation
        try:
            pod2 = FunctionPod(double_input_func, input_count=2)
            stream2 = SyncStreamFromLists(packets, tags)
            result2 = list(pod2(stream2))

            # Should produce fewer outputs since it needs 2 inputs per call
            assert len(result2) <= len(packets)

        except (TypeError, AttributeError):
            # FunctionPod might not support configurable input counts
            pass

    def test_function_pod_with_none_outputs(self, sample_stream):
        """Test function pod with function that sometimes returns None."""

        def conditional_function(inputs):
            packet, tag = inputs[0]
            # Only process strings
            if isinstance(packet, str):
                return f"processed_{packet}"
            return None  # Skip non-strings

        # Mix of string and non-string packets
        packets = ["hello", 42, "world", None, "test"]
        tags = ["str1", "int1", "str2", "null1", "str3"]
        stream = SyncStreamFromLists(packets, tags)

        pod = FunctionPod(conditional_function)
        result_stream = pod(stream)
        result = list(result_stream)

        # Should only process string packets
        string_packets = [p for p in packets if isinstance(p, str)]
        assert len(result) == len(string_packets)

        for packet, _ in result:
            assert packet.startswith("processed_")

    def test_function_pod_stateful_function(self, data_store):
        """Test function pod with stateful function using datastore."""

        def stateful_function(inputs, datastore):
            packet, tag = inputs[0]

            # Keep running total
            if "total" not in datastore:
                datastore["total"] = 0
            if "count" not in datastore:
                datastore["count"] = 0

            if isinstance(packet, (int, float)):
                datastore["total"] += packet
                datastore["count"] += 1
                avg = datastore["total"] / datastore["count"]
                return f"avg_so_far_{avg:.2f}"

            return None

        packets = [10, 20, 30, 40]
        tags = ["n1", "n2", "n3", "n4"]
        stream = SyncStreamFromLists(packets, tags)

        pod = FunctionPod(stateful_function, datastore=data_store)
        result_stream = pod(stream)
        result = list(result_stream)

        # Should produce running averages
        assert len(result) == 4
        assert result[0][0] == "avg_so_far_10.00"  # 10/1
        assert result[1][0] == "avg_so_far_15.00"  # (10+20)/2
        assert result[2][0] == "avg_so_far_20.00"  # (10+20+30)/3
        assert result[3][0] == "avg_so_far_25.00"  # (10+20+30+40)/4

    def test_function_pod_generator_output(self, sample_stream):
        """Test function pod with function that yields multiple outputs."""

        def generator_function(inputs):
            packet, tag = inputs[0]
            # Yield multiple outputs for each input
            for i in range(3):
                yield f"{packet}_part_{i}"

        pod = FunctionPod(generator_function)
        result_stream = pod(sample_stream)
        result = list(result_stream)

        # Should produce 3 outputs per input
        original_packets = list(sample_stream)
        assert len(result) == len(original_packets) * 3

        # Check pattern of outputs
        for i, (packet, tag) in enumerate(result):
            original_idx = i // 3
            part_idx = i % 3
            original_packet = original_packets[original_idx][0]
            expected_packet = f"{original_packet}_part_{part_idx}"
            assert packet == expected_packet

    def test_function_pod_complex_data_transformation(self):
        """Test function pod with complex data transformation."""

        def json_processor(inputs):
            packet, tag = inputs[0]

            if isinstance(packet, dict):
                # Extract all values and create separate outputs
                for key, value in packet.items():
                    yield f"{key}={value}"
            else:
                yield f"non_dict_{packet}"

        packets = [
            {"name": "Alice", "age": 30},
            "simple_string",
            {"x": 1, "y": 2, "z": 3},
        ]
        tags = ["person", "text", "coordinates"]
        stream = SyncStreamFromLists(packets, tags)

        pod = FunctionPod(json_processor)
        result_stream = pod(stream)
        result = list(result_stream)

        # Should extract dict entries
        result_packets = [packet for packet, _ in result]

        assert "name=Alice" in result_packets
        assert "age=30" in result_packets
        assert "non_dict_simple_string" in result_packets
        assert "x=1" in result_packets
        assert "y=2" in result_packets
        assert "z=3" in result_packets

    def test_function_pod_empty_stream(self, func_single_output):
        """Test function pod with empty stream."""
        empty_stream = SyncStreamFromLists([], [])
        pod = FunctionPod(func_single_output)
        result_stream = pod(empty_stream)

        result = list(result_stream)
        assert len(result) == 0

    def test_function_pod_large_stream(self, func_single_output):
        """Test function pod with large stream."""
        packets = [f"packet_{i}" for i in range(1000)]
        tags = [f"tag_{i}" for i in range(1000)]
        stream = SyncStreamFromLists(packets, tags)

        pod = FunctionPod(func_single_output)
        result_stream = pod(stream)

        # Process stream lazily to test memory efficiency
        count = 0
        for packet, tag in result_stream:
            count += 1
            if count == 100:  # Stop early
                break

        assert count == 100

    def test_function_pod_chaining(self, func_single_output):
        """Test chaining function pods."""

        def second_processor(inputs):
            packet, tag = inputs[0]
            return f"second_{packet}"

        packets = ["a", "b", "c"]
        tags = ["t1", "t2", "t3"]
        stream = SyncStreamFromLists(packets, tags)

        # Chain two function pods
        pod1 = FunctionPod(func_single_output)
        pod2 = FunctionPod(second_processor)

        intermediate_stream = pod1(stream)
        final_stream = pod2(intermediate_stream)
        result = list(final_stream)

        # Should apply both transformations
        expected = [
            ("second_processed_a", "t1"),
            ("second_processed_b", "t2"),
            ("second_processed_c", "t3"),
        ]
        assert result == expected
