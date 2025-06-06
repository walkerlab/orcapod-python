"""
Shared fixtures for streams and operations testing.
"""

import tempfile
import json
import numpy as np
from pathlib import Path
from typing import Any, Iterator
import pytest

from orcabridge.types import Tag, Packet
from orcabridge.streams import SyncStreamFromLists
from orcabridge.store import DirDataStore


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_tags():
    """Sample tags for testing."""
    return [
        {"file_name": "day1", "session": "morning"},
        {"file_name": "day2", "session": "afternoon"},
        {"file_name": "day3", "session": "evening"},
    ]


@pytest.fixture
def sample_packets():
    """Sample packets for testing."""
    return [
        {"txt_file": "data/day1.txt", "metadata": "meta1.json"},
        {"txt_file": "data/day2.txt", "metadata": "meta2.json"},
        {"txt_file": "data/day3.txt", "metadata": "meta3.json"},
    ]


@pytest.fixture
def sample_stream(sample_tags, sample_packets):
    """Create a sample stream from tags and packets."""
    return SyncStreamFromLists(
        tags=sample_tags,
        packets=sample_packets,
        tag_keys=["file_name", "session"],
        packet_keys=["txt_file", "metadata"],
    )


@pytest.fixture
def empty_stream() -> SyncStreamFromLists:
    """Create an empty stream."""
    return SyncStreamFromLists(paired=[])


@pytest.fixture
def single_item_stream() -> SyncStreamFromLists:
    """Create a stream with a single item."""
    return SyncStreamFromLists(tags=[{"name": "single"}], packets=[{"data": "value"}])


@pytest.fixture
def test_files(temp_dir) -> dict[str, Any]:
    """Create test files for source testing."""
    # Create text files
    txt_dir = temp_dir / "txt_files"
    txt_dir.mkdir()

    txt_files = []
    for i, day in enumerate(["day1", "day2", "day3"], 1):
        txt_file = txt_dir / f"{day}.txt"
        txt_file.write_text(f"Content for {day}\n" * (i * 5))
        txt_files.append(txt_file)

    # Create binary files with numpy arrays
    bin_dir = temp_dir / "bin_files"
    bin_dir.mkdir()

    bin_files = []
    for i, session in enumerate(["session_day1", "session_day2"], 1):
        bin_file = bin_dir / f"{session}.bin"
        data = np.random.rand(10 * i).astype(np.float64)
        bin_file.write_bytes(data.tobytes())
        bin_files.append(bin_file)

    # Create json files
    json_dir = temp_dir / "json_files"
    json_dir.mkdir()

    json_files = []
    for i, info in enumerate(["info_day1", "info_day2"], 1):
        json_file = json_dir / f"{info}.json"
        data = {"lines": i * 5, "day": f"day{i}", "processed": False}
        json_file.write_text(json.dumps(data))
        json_files.append(json_file)

    return {
        "txt_dir": txt_dir,
        "txt_files": txt_files,
        "bin_dir": bin_dir,
        "bin_files": bin_files,
        "json_dir": json_dir,
        "json_files": json_files,
    }


@pytest.fixture
def data_store(temp_dir) -> DirDataStore:
    """Create a test data store."""
    store_dir = temp_dir / "data_store"
    return DirDataStore(store_dir=store_dir)


# Sample functions for FunctionPod testing


def sample_function_no_output(input_file: str) -> None:
    """Sample function that takes input but returns nothing."""
    pass


def sample_function_single_output(input_file: str) -> str:
    """Sample function that returns a single output."""
    return str(Path(input_file).with_suffix(".processed"))


def sample_function_multiple_outputs(input_file: str) -> tuple[str, str]:
    """Sample function that returns multiple outputs."""
    base = Path(input_file).stem
    return f"{base}_output1.txt", f"{base}_output2.txt"


def sample_function_with_error(input_file: str) -> str:
    """Sample function that raises an error."""
    raise ValueError("Intentional error for testing")


def count_lines_function(txt_file: str) -> int:
    """Function that counts lines in a text file."""
    with open(txt_file, "r") as f:
        return len(f.readlines())


def compute_stats_function(bin_file: str, temp_dir: str | None = None) -> str:
    """Function that computes statistics on binary data."""
    import tempfile

    with open(bin_file, "rb") as f:
        data = np.frombuffer(f.read(), dtype=np.float64)

    stats = {
        "mean": float(np.mean(data)),
        "std": float(np.std(data)),
        "min": float(np.min(data)),
        "max": float(np.max(data)),
        "count": len(data),
    }

    if temp_dir is None:
        output_file = Path(tempfile.mkdtemp()) / "stats.json"
    else:
        output_file = Path(temp_dir) / "stats.json"

    with open(output_file, "w") as f:
        json.dump(stats, f)

    return str(output_file)


# Predicate functions for Filter testing


def filter_by_session_morning(tag: Tag, packet: Packet) -> bool:
    """Filter predicate that keeps only morning sessions."""
    return tag.get("session") == "morning"


def filter_by_filename_pattern(tag: Tag, packet: Packet) -> bool:
    """Filter predicate that keeps files matching a pattern."""
    return "day1" in tag.get("file_name", "")  # type: ignore


# Transform functions


def transform_add_prefix(tag: Tag, packet: Packet) -> tuple[Tag, Packet]:
    """Transform that adds prefix to file_name tag."""
    new_tag = tag.copy()
    if "file_name" in new_tag:
        new_tag["file_name"] = f"prefix_{new_tag['file_name']}"
    return new_tag, packet


def transform_rename_keys(tag: Tag, packet: Packet) -> tuple[Tag, Packet]:
    """Transform that renames packet keys."""
    new_packet = packet.copy()
    if "txt_file" in new_packet:
        new_packet["content"] = new_packet.pop("txt_file")
    return tag, new_packet
