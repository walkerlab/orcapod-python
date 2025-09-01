#!/usr/bin/env python
"""Common test fixtures for store tests."""

import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_files(temp_dir):
    """Create sample files for testing."""
    # Create input files
    input_dir = Path(temp_dir) / "input"
    input_dir.mkdir(exist_ok=True)

    input_file1 = input_dir / "file1.txt"
    with open(input_file1, "w") as f:
        f.write("Sample content 1")

    input_file2 = input_dir / "file2.txt"
    with open(input_file2, "w") as f:
        f.write("Sample content 2")

    # Create output files
    output_dir = Path(temp_dir) / "output"
    output_dir.mkdir(exist_ok=True)

    output_file1 = output_dir / "output1.txt"
    with open(output_file1, "w") as f:
        f.write("Output content 1")

    output_file2 = output_dir / "output2.txt"
    with open(output_file2, "w") as f:
        f.write("Output content 2")

    return {
        "input": {"file1": str(input_file1), "file2": str(input_file2)},
        "output": {"output1": str(output_file1), "output2": str(output_file2)},
    }
