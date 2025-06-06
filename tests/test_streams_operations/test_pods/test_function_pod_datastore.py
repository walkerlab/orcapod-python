"""
Test module for FunctionPod datastore integration.

This module tests FunctionPod functionality when working with datastore operations,
including storage, retrieval, and state management across pod invocations.
"""

import pytest
import tempfile
import os
from pathlib import Path

from orcabridge.pod import FunctionPod
from orcabridge.stream import SyncStreamFromLists


@pytest.fixture
def temp_datastore():
    """Create a temporary datastore directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_stream_data():
    """Sample stream data for testing."""
    return [
        ({"file_id": 1}, {"content": "Hello World", "metadata": {"type": "text"}}),
        ({"file_id": 2}, {"content": "Python Code", "metadata": {"type": "code"}}),
        (
            {"file_id": 3},
            {"content": "Data Analysis", "metadata": {"type": "analysis"}},
        ),
    ]


@pytest.fixture
def sample_stream(sample_stream_data):
    """Create a sample stream."""
    tags, packets = zip(*sample_stream_data)
    return SyncStreamFromLists(list(tags), list(packets))


class TestFunctionPodDatastore:
    """Test cases for FunctionPod datastore integration."""

    def test_datastore_saving_function(self, temp_datastore, sample_stream):
        """Test FunctionPod with function that saves data to datastore."""

        def save_to_datastore(tag, packet, datastore):
            """Save packet content to datastore."""
            file_id = tag["file_id"]
            content = packet["content"]

            # Create file path
            file_path = datastore / f"file_{file_id}.txt"

            # Save content
            with open(file_path, "w") as f:
                f.write(content)

            # Return tag and packet with file path
            return tag, {**packet, "saved_path": str(file_path)}

        # Create pod with datastore
        pod = FunctionPod(save_to_datastore, datastore=temp_datastore)

        # Process stream
        result_stream = pod(sample_stream)
        result = list(result_stream)

        # Check results
        assert len(result) == 3

        # Verify files were created
        for i, (tag, packet) in enumerate(result, 1):
            expected_path = temp_datastore / f"file_{i}.txt"
            assert expected_path.exists()

            # Verify content
            with open(expected_path, "r") as f:
                saved_content = f.read()

            original_content = sample_stream_data[i - 1][1]["content"]
            assert saved_content == original_content

            # Verify packet contains path
            assert "saved_path" in packet
            assert packet["saved_path"] == str(expected_path)

    def test_datastore_loading_function(self, temp_datastore):
        """Test FunctionPod with function that loads data from datastore."""

        # First, create some test files
        test_files = {
            "file1.txt": "Content of file 1",
            "file2.txt": "Content of file 2",
            "file3.txt": "Content of file 3",
        }

        for filename, content in test_files.items():
            file_path = temp_datastore / filename
            with open(file_path, "w") as f:
                f.write(content)

        def load_from_datastore(tag, packet, datastore):
            """Load content from datastore based on filename in packet."""
            filename = packet["filename"]
            file_path = datastore / filename

            if file_path.exists():
                with open(file_path, "r") as f:
                    content = f.read()
                return tag, {**packet, "content": content, "loaded": True}
            else:
                return tag, {**packet, "content": None, "loaded": False}

        # Create input stream with filenames
        tags = [{"request_id": i} for i in range(1, 4)]
        packets = [{"filename": f"file{i}.txt"} for i in range(1, 4)]
        input_stream = SyncStreamFromLists(tags, packets)

        # Create pod with datastore
        pod = FunctionPod(load_from_datastore, datastore=temp_datastore)

        # Process stream
        result_stream = pod(input_stream)
        result = list(result_stream)

        # Check results
        assert len(result) == 3

        for i, (tag, packet) in enumerate(result):
            assert packet["loaded"] is True
            assert packet["content"] == f"Content of file {i + 1}"
            assert packet["filename"] == f"file{i + 1}.txt"

    def test_datastore_with_stateful_operations(self, temp_datastore):
        """Test FunctionPod with stateful operations using datastore."""

        def stateful_counter(tag, packet, datastore):
            """Maintain a counter in datastore across invocations."""
            counter_file = datastore / "counter.txt"

            # Read current counter value
            if counter_file.exists():
                with open(counter_file, "r") as f:
                    count = int(f.read().strip())
            else:
                count = 0

            # Increment counter
            count += 1

            # Save new counter value
            with open(counter_file, "w") as f:
                f.write(str(count))

            return tag, {**packet, "sequence_number": count}

        # Create multiple input streams to test state persistence
        tags1 = [{"batch": 1, "item": i} for i in range(3)]
        packets1 = [{"data": f"item_{i}"} for i in range(3)]
        stream1 = SyncStreamFromLists(tags1, packets1)

        tags2 = [{"batch": 2, "item": i} for i in range(2)]
        packets2 = [{"data": f"item_{i}"} for i in range(2)]
        stream2 = SyncStreamFromLists(tags2, packets2)

        # Create pod with datastore
        pod = FunctionPod(stateful_counter, datastore=temp_datastore)

        # Process first stream
        result1 = list(pod(stream1))

        # Process second stream (should continue counting)
        result2 = list(pod(stream2))

        # Check that counter state persisted across streams
        expected_sequences1 = [1, 2, 3]
        expected_sequences2 = [4, 5]

        for i, (tag, packet) in enumerate(result1):
            assert packet["sequence_number"] == expected_sequences1[i]

        for i, (tag, packet) in enumerate(result2):
            assert packet["sequence_number"] == expected_sequences2[i]

    def test_datastore_error_handling(self, temp_datastore):
        """Test error handling when datastore operations fail."""

        def failing_datastore_operation(tag, packet, datastore):
            """Function that tries to access non-existent file."""
            nonexistent_file = datastore / "nonexistent.txt"

            # This should raise an exception
            with open(nonexistent_file, "r") as f:
                content = f.read()

            return tag, {**packet, "content": content}

        tags = [{"id": 1}]
        packets = [{"data": "test"}]
        stream = SyncStreamFromLists(tags, packets)

        pod = FunctionPod(failing_datastore_operation, datastore=temp_datastore)
        result_stream = pod(stream)

        # Should propagate the file not found error
        with pytest.raises(FileNotFoundError):
            list(result_stream)

    def test_datastore_with_subdirectories(self, temp_datastore):
        """Test FunctionPod with datastore operations using subdirectories."""

        def organize_by_type(tag, packet, datastore):
            """Organize files by type in subdirectories."""
            file_type = packet["type"]
            content = packet["content"]
            file_id = tag["id"]

            # Create subdirectory
            type_dir = datastore / file_type
            type_dir.mkdir(exist_ok=True)

            # Save file in subdirectory
            file_path = type_dir / f"{file_id}.txt"
            with open(file_path, "w") as f:
                f.write(content)

            return tag, {**packet, "organized_path": str(file_path)}

        # Create input with different types
        tags = [{"id": f"file_{i}"} for i in range(4)]
        packets = [
            {"type": "documents", "content": "Document content 1"},
            {"type": "images", "content": "Image metadata 1"},
            {"type": "documents", "content": "Document content 2"},
            {"type": "code", "content": "Python code"},
        ]
        stream = SyncStreamFromLists(tags, packets)

        pod = FunctionPod(organize_by_type, datastore=temp_datastore)
        result = list(pod(stream))

        # Check that subdirectories were created
        assert (temp_datastore / "documents").exists()
        assert (temp_datastore / "images").exists()
        assert (temp_datastore / "code").exists()

        # Check that files were saved in correct subdirectories
        assert (temp_datastore / "documents" / "file_0.txt").exists()
        assert (temp_datastore / "images" / "file_1.txt").exists()
        assert (temp_datastore / "documents" / "file_2.txt").exists()
        assert (temp_datastore / "code" / "file_3.txt").exists()

    def test_datastore_without_datastore_param(self):
        """Test that function without datastore parameter works normally."""

        def simple_function(tag, packet):
            """Function that doesn't use datastore."""
            return tag, {**packet, "processed": True}

        # This should work even though we don't provide datastore
        pod = FunctionPod(simple_function)

        tags = [{"id": 1}]
        packets = [{"data": "test"}]
        stream = SyncStreamFromLists(tags, packets)

        result = list(pod(stream))
        assert len(result) == 1
        assert result[0][1]["processed"] is True

    def test_datastore_metadata_operations(self, temp_datastore):
        """Test FunctionPod with metadata tracking in datastore."""

        def track_processing_metadata(tag, packet, datastore):
            """Track processing metadata for each item."""
            import time
            import json

            item_id = tag["id"]
            processing_time = time.time()

            # Create metadata entry
            metadata = {
                "item_id": item_id,
                "processed_at": processing_time,
                "original_data": packet["data"],
                "processing_status": "completed",
            }

            # Save metadata
            metadata_file = datastore / f"metadata_{item_id}.json"
            with open(metadata_file, "w") as f:
                json.dump(metadata, f)

            return tag, {**packet, "metadata_file": str(metadata_file)}

        tags = [{"id": f"item_{i}"} for i in range(3)]
        packets = [{"data": f"data_{i}"} for i in range(3)]
        stream = SyncStreamFromLists(tags, packets)

        pod = FunctionPod(track_processing_metadata, datastore=temp_datastore)
        result = list(pod(stream))

        # Check that metadata files were created
        for i in range(3):
            metadata_file = temp_datastore / f"metadata_item_{i}.json"
            assert metadata_file.exists()

            # Verify metadata content
            import json

            with open(metadata_file, "r") as f:
                metadata = json.load(f)

            assert metadata["item_id"] == f"item_{i}"
            assert metadata["original_data"] == f"data_{i}"
            assert metadata["processing_status"] == "completed"
            assert "processed_at" in metadata

    def test_datastore_with_generator_function(self, temp_datastore):
        """Test FunctionPod with generator function that uses datastore."""

        def split_and_save(tag, packet, datastore):
            """Split content and save each part separately."""
            content = packet["content"]
            parts = content.split()
            base_id = tag["id"]

            for i, part in enumerate(parts):
                part_id = f"{base_id}_part_{i}"

                # Save part to datastore
                part_file = datastore / f"{part_id}.txt"
                with open(part_file, "w") as f:
                    f.write(part)

                # Yield new tag-packet pair
                new_tag = {**tag, "part_id": part_id, "part_index": i}
                new_packet = {"part_content": part, "saved_to": str(part_file)}
                yield new_tag, new_packet

        tags = [{"id": "doc1"}]
        packets = [{"content": "Hello World Python Programming"}]
        stream = SyncStreamFromLists(tags, packets)

        pod = FunctionPod(split_and_save, datastore=temp_datastore)
        result = list(pod(stream))

        # Should have 4 parts
        assert len(result) == 4

        expected_parts = ["Hello", "World", "Python", "Programming"]
        for i, (tag, packet) in enumerate(result):
            assert tag["part_index"] == i
            assert packet["part_content"] == expected_parts[i]

            # Check that file was saved
            saved_file = Path(packet["saved_to"])
            assert saved_file.exists()

            with open(saved_file, "r") as f:
                saved_content = f.read()
            assert saved_content == expected_parts[i]

    def test_datastore_path_validation(self, temp_datastore):
        """Test that datastore path is properly validated and accessible."""

        def check_datastore_access(tag, packet, datastore):
            """Function that checks datastore accessibility."""
            # Check if datastore is a Path object
            assert isinstance(datastore, Path)

            # Check if datastore directory exists and is writable
            assert datastore.exists()
            assert datastore.is_dir()

            # Test writing and reading
            test_file = datastore / "access_test.txt"
            with open(test_file, "w") as f:
                f.write("test")

            with open(test_file, "r") as f:
                content = f.read()

            assert content == "test"

            # Clean up
            test_file.unlink()

            return tag, {**packet, "datastore_accessible": True}

        tags = [{"id": 1}]
        packets = [{"data": "test"}]
        stream = SyncStreamFromLists(tags, packets)

        pod = FunctionPod(check_datastore_access, datastore=temp_datastore)
        result = list(pod(stream))

        assert result[0][1]["datastore_accessible"] is True
