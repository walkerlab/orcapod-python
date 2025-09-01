import hashlib
import os

import pyarrow as pa

from orcapod.protocols.hashing_protocols import (
    FileContentHasher,
    SemanticTypeHasher,
    StringCacher,
)


class PathHasher(SemanticTypeHasher):
    """Hasher for Path semantic type columns - hashes file contents."""

    def __init__(
        self,
        file_hasher: FileContentHasher,
        handle_missing: str = "error",
        string_cacher: StringCacher | None = None,
        cache_key_prefix: str = "path_hasher",
    ):
        """
        Initialize PathHasher.

        Args:
            chunk_size: Size of chunks to read files in bytes
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
        """
        self.file_hasher = file_hasher
        self.handle_missing = handle_missing
        self.cacher = string_cacher
        self.cache_key_prefix = cache_key_prefix

    def _hash_file_content(self, file_path: str) -> bytes:
        """Hash the content of a single file"""
        import os

        # if cacher exists, check if the hash is cached
        if self.cacher:
            cache_key = f"{self.cache_key_prefix}:{file_path}"
            cached_hash_hex = self.cacher.get_cached(cache_key)
            if cached_hash_hex is not None:
                return bytes.fromhex(cached_hash_hex)

        try:
            if not os.path.exists(file_path):
                if self.handle_missing == "error":
                    raise FileNotFoundError(f"File not found: {file_path}")
                elif self.handle_missing == "skip":
                    return hashlib.sha256(b"<FILE_NOT_FOUND>").digest()
                elif self.handle_missing == "null_hash":
                    return hashlib.sha256(b"<FILE_NOT_FOUND>").digest()

            hashed_value = self.file_hasher.hash_file(file_path)
            if self.cacher:
                # Cache the computed hash hex
                self.cacher.set_cached(
                    f"{self.cache_key_prefix}:{file_path}", hashed_value.to_hex()
                )
            # TODO: make consistent use of bytes/string for hash
            return hashed_value.digest

        except (IOError, OSError, PermissionError) as e:
            if self.handle_missing == "error":
                raise IOError(f"Cannot read file {file_path}: {e}")
            else:  # skip or null_hash
                error_msg = f"<FILE_ERROR:{type(e).__name__}>"
                return hashlib.sha256(error_msg.encode("utf-8")).digest()

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

    def set_cacher(self, cacher: StringCacher) -> None:
        """
        Add a string cacher for caching hash values.
        This is a no-op for PathHasher since it hashes file contents directly.
        """
        # PathHasher does not use string caching, so this is a no-op
        self.cacher = cacher
