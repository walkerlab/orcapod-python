import hashlib
from typing import Any
import pyarrow as pa
import polars as pl
import json
from orcapod.hashing.types import SemanticTypeHasher, StringCacher
from orcapod.hashing import arrow_serialization
from collections.abc import Callable

SERIALIZATION_METHOD_LUT: dict[str, Callable[[pa.Table], bytes]] = {
    "logical": arrow_serialization.serialize_table_logical,
}


def serialize_pyarrow_table(table: pa.Table) -> str:
    """
    Serialize a PyArrow table to a stable JSON string by converting to dictionary of lists.

    Args:
        table: PyArrow table to serialize

    Returns:
        JSON string representation with sorted keys and no whitespace
    """
    # Convert table to dictionary of lists using to_pylist()
    data_dict = {}

    for column_name in table.column_names:
        # Convert Arrow column to Python list, which visits all elements
        data_dict[column_name] = table.column(column_name).to_pylist()

    # Serialize to JSON with sorted keys and no whitespace
    return json.dumps(
        data_dict,
        separators=(",", ":"),
        sort_keys=True,
    )


class SemanticArrowHasher:
    """
    Stable hasher for Arrow tables with semantic type support.

    This hasher:
    1. Processes columns with special semantic types using dedicated hashers
    2. Sorts columns by name for deterministic ordering
    3. Uses Arrow IPC format for stable serialization
    4. Computes final hash of the processed packet
    """

    def __init__(
        self,
        hasher_id: str,
        hash_algorithm: str = "sha256",
        chunk_size: int = 8192,
        handle_missing: str = "error",
        semantic_type_hashers: dict[str, SemanticTypeHasher] | None = None,
        serialization_method: str = "logical",
    ):
        """
        Initialize SemanticArrowHasher.

        Args:
            chunk_size: Size of chunks to read files in bytes
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
        """
        self._hasher_id = hasher_id
        self.chunk_size = chunk_size
        self.handle_missing = handle_missing
        self.semantic_type_hashers: dict[str, SemanticTypeHasher] = (
            semantic_type_hashers or {}
        )
        self.hash_algorithm = hash_algorithm
        if serialization_method not in SERIALIZATION_METHOD_LUT:
            raise ValueError(
                f"Invalid serialization method '{serialization_method}'. "
                f"Supported methods: {list(SERIALIZATION_METHOD_LUT.keys())}"
            )
        self.serialization_method = serialization_method
        self._serialize_arrow_table = SERIALIZATION_METHOD_LUT[serialization_method]

    def set_cacher(self, semantic_type: str, cacher: StringCacher) -> None:
        """
        Add a string cacher for caching hash values.

        This is a no-op for SemanticArrowHasher since it hashes column contents directly.
        """
        if semantic_type in self.semantic_type_hashers:
            self.semantic_type_hashers[semantic_type].set_cacher(cacher)
        else:
            raise KeyError(f"No hasher registered for semantic type '{semantic_type}'")

    def get_hasher_id(self) -> str:
        return self._hasher_id

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

    # def _serialize_table_ipc(self, table: pa.Table) -> bytes:
    #     # TODO: fix and use logical table hashing instead
    #     """Serialize table using Arrow IPC format for stable binary representation."""
    #     buffer = BytesIO()

    #     # Use IPC stream format for deterministic serialization
    #     with ipc.new_stream(buffer, table.schema) as writer:
    #         writer.write_table(table)

    #     return buffer.getvalue()

    def hash_table(self, table: pa.Table, prefix_hasher_id: bool = True) -> str:
        """
        Compute stable hash of Arrow table.

        Args:
            table: Arrow table to hash

        Returns:
            Hex string of the computed hash
        """

        # Step 1: Process columns with semantic types
        processed_table = self._process_table_columns(table)

        # Step 2: Sort columns by name for deterministic ordering
        sorted_table = self._sort_table_columns(processed_table)

        # normalize all string to large strings by passing through polars
        # TODO: consider cleaner approach in the future
        sorted_table = pl.DataFrame(sorted_table).to_arrow()

        # Step 3: Serialize using Arrow IPC format
        serialized_bytes = self._serialize_arrow_table(sorted_table)

        # Step 4: Compute final hash
        hasher = hashlib.new(self.hash_algorithm)
        hasher.update(serialized_bytes)

        hash_str = hasher.hexdigest()
        if prefix_hasher_id:
            hash_str = f"{self.get_hasher_id()}@{hash_str}"

        return hash_str

    def hash_table_with_metadata(self, table: pa.Table) -> dict[str, Any]:
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
        table_hash = self.hash_table(table)

        return {
            "hash": table_hash,
            "num_rows": len(table),
            "num_columns": len(table.schema),
            "processed_columns": processed_columns,
            "column_order": [field.name for field in table.schema],
        }
