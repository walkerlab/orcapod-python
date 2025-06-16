import hashlib
import os
from typing import Any, Protocol
from abc import ABC, abstractmethod
import pyarrow as pa
import pyarrow.ipc as ipc
from io import BytesIO


class SemanticTypeHasher(Protocol):
    """Abstract base class for semantic type-specific hashers."""

    @abstractmethod
    def hash_column(self, column: pa.Array) -> bytes:
        """Hash a column with this semantic type and return the hash bytes."""
        pass


class PathHasher(SemanticTypeHasher):
    """Hasher for Path semantic type columns - hashes file contents."""

    def __init__(self, chunk_size: int = 8192, handle_missing: str = "error"):
        """
        Initialize PathHasher.

        Args:
            chunk_size: Size of chunks to read files in bytes
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
        """
        self.chunk_size = chunk_size
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

            hasher = hashlib.sha256()

            # Read file in chunks to handle large files efficiently
            with open(file_path, "rb") as f:
                while chunk := f.read(self.chunk_size):
                    hasher.update(chunk)

            return hasher.hexdigest()

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


class SemanticArrowHasher:
    """
    Stable hasher for Arrow tables with semantic type support.

    This hasher:
    1. Processes columns with special semantic types using dedicated hashers
    2. Sorts columns by name for deterministic ordering
    3. Uses Arrow IPC format for stable serialization
    4. Computes final hash of the processed packet
    """

    def __init__(self, chunk_size: int = 8192, handle_missing: str = "error"):
        """
        Initialize SemanticArrowHasher.

        Args:
            chunk_size: Size of chunks to read files in bytes
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
        """
        self.chunk_size = chunk_size
        self.handle_missing = handle_missing
        self.semantic_type_hashers: dict[str, SemanticTypeHasher] = {}

    def register_semantic_hasher(self, semantic_type: str, hasher: SemanticTypeHasher):
        """Register a custom hasher for a semantic type."""
        self.semantic_type_hashers[semantic_type] = hasher

    def _get_semantic_type(self, field: pa.Field) -> str | None:
        """Extract semantic_type from field metadata."""
        if field.metadata is None:
            return None

        metadata = field.metadata
        if b"semantic_type" in metadata:
            return metadata[b"semantic_type"].decode("utf-8")
        elif "semantic_type" in metadata:
            return metadata["semantic_type"]

        return None

    def _create_hash_column(
        self, original_column: pa.Array, hash_bytes: bytes, original_field: pa.Field
    ) -> tuple[pa.Array, pa.Field]:
        """Create a new column containing the hash bytes."""
        # Create array of hash bytes (one hash value repeated for each row)
        hash_value = hash_bytes.hex()  # Convert to hex string for readability
        hash_array = pa.array([hash_value] * len(original_column))

        # Create new field with modified metadata
        new_metadata = dict(original_field.metadata) if original_field.metadata else {}
        new_metadata["original_semantic_type"] = new_metadata.get(
            "semantic_type", "unknown"
        )
        new_metadata["semantic_type"] = "hash"
        new_metadata["hash_algorithm"] = "sha256"

        new_field = pa.field(
            original_field.name,
            pa.string(),  # Hash stored as string
            nullable=original_field.nullable,
            metadata=new_metadata,
        )

        return hash_array, new_field

    def _process_table_columns(self, table: pa.Table) -> pa.Table:
        # TODO: add copy of table-level metadata to the new table
        """Process table columns, replacing semantic type columns with their hashes."""
        new_columns = []
        new_fields = []

        for i, field in enumerate(table.schema):
            column = table.column(i)
            semantic_type = self._get_semantic_type(field)

            if semantic_type in self.semantic_type_hashers:
                # Hash the column using the appropriate semantic hasher
                hasher = self.semantic_type_hashers[semantic_type]
                hash_bytes = hasher.hash_column(column)

                # Replace column with hash
                hash_column, hash_field = self._create_hash_column(
                    column, hash_bytes, field
                )
                new_columns.append(hash_column)
                new_fields.append(hash_field)
            else:
                # Keep original column
                new_columns.append(column)
                new_fields.append(field)

        # Create new table with processed columns
        new_schema = pa.schema(new_fields)
        return pa.table(new_columns, schema=new_schema)

    def _sort_table_columns(self, table: pa.Table) -> pa.Table:
        """Sort table columns by field name for deterministic ordering."""
        # Get column indices sorted by field name
        sorted_indices = sorted(
            range(len(table.schema)), key=lambda i: table.schema.field(i).name
        )

        # Reorder columns
        sorted_columns = [table.column(i) for i in sorted_indices]
        sorted_fields = [table.schema.field(i) for i in sorted_indices]

        sorted_schema = pa.schema(sorted_fields)
        return pa.table(sorted_columns, schema=sorted_schema)

    def _serialize_table_ipc(self, table: pa.Table) -> bytes:
        """Serialize table using Arrow IPC format for stable binary representation."""
        buffer = BytesIO()

        # Use IPC stream format for deterministic serialization
        with ipc.new_stream(buffer, table.schema) as writer:
            writer.write_table(table)

        return buffer.getvalue()

    def hash_table(self, table: pa.Table, algorithm: str = "sha256") -> str:
        """
        Compute stable hash of Arrow table.

        Args:
            table: Arrow table to hash
            algorithm: Hash algorithm to use ('sha256', 'md5', etc.)

        Returns:
            Hex string of the computed hash
        """
        # Step 1: Process columns with semantic types
        processed_table = self._process_table_columns(table)

        # Step 2: Sort columns by name for deterministic ordering
        sorted_table = self._sort_table_columns(processed_table)

        # Step 3: Serialize using Arrow IPC format
        serialized_bytes = self._serialize_table_ipc(sorted_table)

        # Step 4: Compute final hash
        hasher = hashlib.new(algorithm)
        hasher.update(serialized_bytes)

        return hasher.hexdigest()

    def hash_table_with_metadata(
        self, table: pa.Table, algorithm: str = "sha256"
    ) -> dict[str, Any]:
        """
        Compute hash with additional metadata about the process.

        Returns:
            Dictionary containing hash, metadata, and processing info
        """
        processed_columns = []

        # Track processing steps
        for i, field in enumerate(table.schema):
            semantic_type = self._get_semantic_type(field)
            column_info = {
                "name": field.name,
                "original_type": str(field.type),
                "semantic_type": semantic_type,
                "processed": semantic_type in self.semantic_type_hashers,
            }
            processed_columns.append(column_info)

        # Compute hash
        table_hash = self.hash_table(table, algorithm)

        return {
            "hash": table_hash,
            "algorithm": algorithm,
            "num_rows": len(table),
            "num_columns": len(table.schema),
            "processed_columns": processed_columns,
            "column_order": [field.name for field in table.schema],
        }
