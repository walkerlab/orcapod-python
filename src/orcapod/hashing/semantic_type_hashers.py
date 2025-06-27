from .types import SemanticTypeHasher, FileHasher
import os
import hashlib
import pyarrow as pa


class PathHasher(SemanticTypeHasher):
    """Hasher for Path semantic type columns - hashes file contents."""

    def __init__(self, file_hasher: FileHasher, handle_missing: str = "error"):
        """
        Initialize PathHasher.

        Args:
            chunk_size: Size of chunks to read files in bytes
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
        """
        self.file_hasher = file_hasher
        self.handle_missing = handle_missing

    def _hash_file_content(self, file_path: str) -> str:
        """Hash the content of a single file and return hex string."""
        import os

        try:
            if not os.path.exists(file_path):
                if self.handle_missing == "error":
                    raise FileNotFoundError(f"File not found: {file_path}")
                elif self.handle_missing == "skip":
                    return hashlib.sha256(b"<FILE_NOT_FOUND>").hexdigest()
                elif self.handle_missing == "null_hash":
                    return hashlib.sha256(b"<FILE_NOT_FOUND>").hexdigest()

            return self.file_hasher.hash_file(file_path).hex()

        except (IOError, OSError, PermissionError) as e:
            if self.handle_missing == "error":
                raise IOError(f"Cannot read file {file_path}: {e}")
            else:  # skip or null_hash
                error_msg = f"<FILE_ERROR:{type(e).__name__}>"
                return hashlib.sha256(error_msg.encode("utf-8")).hexdigest()

    def hash_column(self, column: pa.Array) -> pa.Array:
        """
        Replace path column with file content hashes.
        Returns a new array where each path is replaced with its file content hash.
        """

        # Convert to python list for processing
        paths = column.to_pylist()

        # Hash each file's content individually
        content_hashes = []
        for path in paths:
            if path is not None:
                # Normalize path for consistency
                normalized_path = os.path.normpath(str(path))
                file_content_hash = self._hash_file_content(normalized_path)
                content_hashes.append(file_content_hash)
            else:
                content_hashes.append(None)  # Preserve nulls

        # Return new array with content hashes instead of paths
        return pa.array(content_hashes)
