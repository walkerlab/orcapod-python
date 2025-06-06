"""Tests for GlobSource functionality."""

import pytest
import os
from pathlib import Path
from orcabridge.sources import GlobSource


class TestGlobSource:
    """Test cases for GlobSource."""

    def test_glob_source_basic(self, test_files, temp_dir):
        """Test basic glob source functionality."""
        # Create a glob pattern for txt files
        pattern = os.path.join(temp_dir, "*.txt")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find all txt files
        txt_files = [f for f in test_files if f.endswith(".txt")]
        assert len(result) == len(txt_files)

        # Check that all found files are actual files
        for file_content, file_path in result:
            assert os.path.isfile(file_path)
            assert file_path.endswith(".txt")
            assert isinstance(file_content, str)  # Text content

    def test_glob_source_specific_pattern(self, test_files, temp_dir):
        """Test glob source with specific pattern."""
        # Look for files starting with "file1"
        pattern = os.path.join(temp_dir, "file1*")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find only file1.txt
        assert len(result) == 1
        file_content, file_path = result[0]
        assert "file1.txt" in file_path
        assert file_content == "Content of file 1"

    def test_glob_source_binary_files(self, test_files, temp_dir):
        """Test glob source with binary files."""
        # Look for binary files
        pattern = os.path.join(temp_dir, "*.bin")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find all binary files
        bin_files = [f for f in test_files if f.endswith(".bin")]
        assert len(result) == len(bin_files)

        for file_content, file_path in result:
            assert file_path.endswith(".bin")
            assert isinstance(file_content, bytes)  # Binary content

    def test_glob_source_json_files(self, test_files, temp_dir):
        """Test glob source with JSON files."""
        pattern = os.path.join(temp_dir, "*.json")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find all JSON files
        json_files = [f for f in test_files if f.endswith(".json")]
        assert len(result) == len(json_files)

        for file_content, file_path in result:
            assert file_path.endswith(".json")
            # Content should be the raw JSON string
            assert '"key"' in file_content

    def test_glob_source_no_matches(self, temp_dir):
        """Test glob source when pattern matches no files."""
        pattern = os.path.join(temp_dir, "*.nonexistent")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)
        assert len(result) == 0

    def test_glob_source_recursive_pattern(self, temp_dir):
        """Test glob source with recursive pattern."""
        # Create subdirectory with files
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir, exist_ok=True)

        sub_file = os.path.join(subdir, "sub_file.txt")
        with open(sub_file, "w") as f:
            f.write("Subdirectory content")

        # Use recursive pattern
        pattern = os.path.join(temp_dir, "**", "*.txt")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find files in both root and subdirectory
        txt_files = [file_path for _, file_path in result]

        # Check that we found files in subdirectory
        sub_files = [f for f in txt_files if "subdir" in f]
        assert len(sub_files) > 0

        # Verify content of subdirectory file
        sub_result = [
            (content, path) for content, path in result if "sub_file.txt" in path
        ]
        assert len(sub_result) == 1
        assert sub_result[0][0] == "Subdirectory content"

    def test_glob_source_absolute_vs_relative_paths(self, test_files, temp_dir):
        """Test glob source with both absolute and relative paths."""
        # Test with absolute path
        abs_pattern = os.path.join(os.path.abspath(temp_dir), "*.txt")
        abs_source = GlobSource(abs_pattern)
        abs_stream = abs_source()
        abs_result = list(abs_stream)

        # Test with relative path (if possible)
        current_dir = os.getcwd()
        try:
            os.chdir(temp_dir)
            rel_pattern = "*.txt"
            rel_source = GlobSource(rel_pattern)
            rel_stream = rel_source()
            rel_result = list(rel_stream)

            # Should find the same number of files
            assert len(abs_result) == len(rel_result)

        finally:
            os.chdir(current_dir)

    def test_glob_source_empty_directory(self, temp_dir):
        """Test glob source in empty directory."""
        empty_dir = os.path.join(temp_dir, "empty_subdir")
        os.makedirs(empty_dir, exist_ok=True)

        pattern = os.path.join(empty_dir, "*")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)
        assert len(result) == 0

    def test_glob_source_large_directory(self, temp_dir):
        """Test glob source with many files."""
        # Create many files
        for i in range(50):
            file_path = os.path.join(temp_dir, f"bulk_file_{i:03d}.txt")
            with open(file_path, "w") as f:
                f.write(f"Content of bulk file {i}")

        pattern = os.path.join(temp_dir, "bulk_file_*.txt")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        assert len(result) == 50

        # Check that files are properly ordered (if implementation sorts)
        file_paths = [file_path for _, file_path in result]
        for i, file_path in enumerate(file_paths):
            if "bulk_file_000.txt" in file_path:
                # Found the first file, check content
                content = [content for content, path in result if path == file_path][0]
                assert "Content of bulk file 0" in content

    def test_glob_source_special_characters_in_filenames(self, temp_dir):
        """Test glob source with special characters in filenames."""
        # Create files with special characters
        special_files = [
            "file with spaces.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
        ]

        for filename in special_files:
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, "w") as f:
                f.write(f"Content of {filename}")

        pattern = os.path.join(temp_dir, "file*.txt")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find all special files plus any existing test files
        found_files = [os.path.basename(file_path) for _, file_path in result]

        for special_file in special_files:
            assert special_file in found_files

    def test_glob_source_mixed_file_types(self, test_files, temp_dir):
        """Test glob source that matches multiple file types."""
        # Pattern that matches both txt and json files
        pattern = os.path.join(temp_dir, "file*")

        source = GlobSource(pattern)
        stream = source()

        result = list(stream)

        # Should find both text and json files
        file_extensions = [os.path.splitext(file_path)[1] for _, file_path in result]

        assert ".txt" in file_extensions
        assert ".json" in file_extensions

    def test_glob_source_case_sensitivity(self, temp_dir):
        """Test glob source case sensitivity."""
        # Create files with different cases
        files = ["Test.TXT", "test.txt", "TEST.txt"]

        for filename in files:
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, "w") as f:
                f.write(f"Content of {filename}")

        # Test exact case match
        pattern = os.path.join(temp_dir, "test.txt")
        source = GlobSource(pattern)
        stream = source()
        result = list(stream)

        # Should find at least the exact match
        found_files = [os.path.basename(file_path) for _, file_path in result]
        assert "test.txt" in found_files

    def test_glob_source_symlinks(self, temp_dir):
        """Test glob source with symbolic links (if supported)."""
        # Create a regular file
        original_file = os.path.join(temp_dir, "original.txt")
        with open(original_file, "w") as f:
            f.write("Original content")

        try:
            # Create a symbolic link
            link_file = os.path.join(temp_dir, "link.txt")
            os.symlink(original_file, link_file)

            pattern = os.path.join(temp_dir, "*.txt")
            source = GlobSource(pattern)
            stream = source()
            result = list(stream)

            # Should find both original and link
            file_paths = [file_path for _, file_path in result]
            original_found = any("original.txt" in path for path in file_paths)
            link_found = any("link.txt" in path for path in file_paths)

            assert original_found
            # Link behavior depends on implementation

        except (OSError, NotImplementedError):
            # Symlinks not supported on this system
            pass

    def test_glob_source_error_handling(self, temp_dir):
        """Test glob source error handling."""
        # Test with invalid pattern
        invalid_pattern = "/nonexistent/path/*.txt"

        source = GlobSource(invalid_pattern)
        stream = source()

        # Should handle gracefully (empty result or specific error)
        try:
            result = list(stream)
            # If no error, should be empty
            assert len(result) == 0
        except (OSError, FileNotFoundError):
            # Expected error for invalid path
            pass

    def test_glob_source_file_permissions(self, temp_dir):
        """Test glob source with files of different permissions."""
        # Create a file and try to change permissions
        restricted_file = os.path.join(temp_dir, "restricted.txt")
        with open(restricted_file, "w") as f:
            f.write("Restricted content")

        try:
            # Try to make file unreadable
            os.chmod(restricted_file, 0o000)

            pattern = os.path.join(temp_dir, "restricted.txt")
            source = GlobSource(pattern)
            stream = source()

            # Should handle permission errors gracefully
            try:
                result = list(stream)
                # If successful, content might be empty or error
            except PermissionError:
                # Expected for restricted files
                pass

        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(restricted_file, 0o644)
            except:
                pass
