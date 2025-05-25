#!/usr/bin/env python
# filepath: /home/eywalker/workspace/orcabridge/tests/test_hashing/reorganize_files.py
"""
Reorganize files in the test_hashing directory.

This script moves files from their current locations to the new
organized directory structure within hash_samples.
"""

import shutil
from pathlib import Path


def reorganize_files():
    """Move files to their new locations."""
    # Get the current directory
    current_dir = Path(__file__).parent

    # Create hash_samples directory if it doesn't exist
    hash_samples_dir = current_dir / "hash_samples"
    hash_samples_dir.mkdir(exist_ok=True)

    # Create subdirectories
    data_structures_dir = hash_samples_dir / "data_structures"
    data_structures_dir.mkdir(exist_ok=True)

    file_samples_dir = hash_samples_dir / "file_samples"
    file_samples_dir.mkdir(exist_ok=True)

    # Move existing hash examples to data_structures
    for file in hash_samples_dir.glob("hash_examples_*.json"):
        target_path = data_structures_dir / file.name
        print(
            f"Moving {file.relative_to(current_dir)} to {target_path.relative_to(current_dir)}"
        )
        shutil.move(str(file), str(target_path))

    # Move sample files to file_samples
    sample_files_dir = current_dir / "sample_files"
    if sample_files_dir.exists():
        for file in sample_files_dir.glob("*"):
            target_path = file_samples_dir / file.name
            print(
                f"Moving {file.relative_to(current_dir)} to {target_path.relative_to(current_dir)}"
            )
            shutil.move(str(file), str(target_path))

        # Remove the old directory if empty
        if not list(sample_files_dir.glob("*")):
            print(
                f"Removing empty directory {sample_files_dir.relative_to(current_dir)}"
            )
            sample_files_dir.rmdir()

    # Move file_hash_lut.json
    file_hash_lut = current_dir / "file_hash_lut.json"
    if file_hash_lut.exists():
        target_path = hash_samples_dir / "file_hash_lut.json"
        print(
            f"Moving {file_hash_lut.relative_to(current_dir)} to {target_path.relative_to(current_dir)}"
        )
        shutil.move(str(file_hash_lut), str(target_path))

    print("File reorganization complete!")


if __name__ == "__main__":
    reorganize_files()
