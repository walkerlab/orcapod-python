import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import threading
from pathlib import Path
from typing import Any, cast
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from orcapod.databases.types import DuplicateError
from pathlib import Path

# Module-level logger
logger = logging.getLogger(__name__)


class MockArrowDataStore:
    """
    Mock Arrow data store for testing purposes.
    This class simulates the behavior of ArrowDataStore without actually saving anything.
    It is useful for unit tests where you want to avoid any I/O operations or when you need
    to test the behavior of your code without relying on external systems. If you need some
    persistence of saved data, consider using SimpleParquetDataStore without providing a
    file path instead.
    """

    def __init__(self):
        logger.info("Initialized MockArrowDataStore")

    def add_record(
        self,
        source_pathh: tuple[str, ...],
        source_id: str,
        entry_id: str,
        arrow_data: pa.Table,
    ) -> pa.Table:
        """Add a record to the mock store."""
        return arrow_data

    def get_record(
        self, source_path: tuple[str, ...], source_id: str, entry_id: str
    ) -> pa.Table | None:
        """Get a specific record."""
        return None

    def get_all_records(
        self, source_path: tuple[str, ...], source_id: str
    ) -> pa.Table | None:
        """Retrieve all records for a given source as a single table."""
        return None

    def get_all_records_as_polars(
        self, source_path: tuple[str, ...], source_id: str
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a single Polars LazyFrame."""
        return None

    def get_records_by_ids(
        self,
        source_path: tuple[str, ...],
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve records by entry IDs as a single table.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Arrow table containing all found records, or None if no records found
        """
        return None

    def get_records_by_ids_as_polars(
        self,
        source_path: tuple[str, ...],
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        return None


class SimpleParquetDataStore:
    """
    Simple Parquet-based Arrow data store, primarily to be used for development purposes.
    If no file path is provided, it will not save anything to disk. Instead, all data will be stored in memory.
    If a file path is provided, it will save data to a single Parquet files in a directory structure reflecting
    the provided source_path. To speed up the process, data will be stored in memory and only saved to disk
    when the `flush` method is called. If used as part of pipeline, flush is automatically called
    at the end of pipeline execution.
    Note that this store provides only very basic functionality and is not suitable for production use.
    For each distinct source_path, only a single parquet file is created to store all data entries.
    Appending is not efficient as it requires reading the entire file into the memory, appending new data,
    and then writing the entire file back to disk. This is not suitable for large datasets or frequent updates.
    However, for development/testing purposes, this data store provides a simple way to store and retrieve
    data without the overhead of a full database or file system and provides very high performance.
    """

    def __init__(
        self, path: str | Path | None = None, duplicate_entry_behavior: str = "error"
    ):
        """
        Initialize the InMemoryArrowDataStore.

        Args:
            duplicate_entry_behavior: How to handle duplicate entry_ids:
                - 'error': Raise ValueError when entry_id already exists
                - 'overwrite': Replace existing entry with new data
        """
        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")
        self.duplicate_entry_behavior = duplicate_entry_behavior

        # Store Arrow tables: {source_key: {entry_id: arrow_table}}
        self._in_memory_store: dict[str, dict[str, pa.Table]] = {}
        logger.info(
            f"Initialized InMemoryArrowDataStore with duplicate_entry_behavior='{duplicate_entry_behavior}'"
        )
        self.base_path = Path(path) if path else None
        if self.base_path:
            try:
                self.base_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Error creating base path {self.base_path}: {e}")

    def _get_source_key(self, source_path: tuple[str, ...]) -> str:
        """Generate key for source storage."""
        return "/".join(source_path)

    def add_record(
        self,
        source_path: tuple[str, ...],
        entry_id: str,
        arrow_data: pa.Table,
        ignore_duplicate: bool = False,
    ) -> pa.Table:
        """
        Add a record to the in-memory store.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_id: Unique identifier for this record
            arrow_data: The Arrow table data to store

        Returns:
            arrow_data equivalent to having loaded the corresponding entry that was just saved

        Raises:
            ValueError: If entry_id already exists and duplicate_entry_behavior is 'error'
        """
        source_key = self._get_source_key(source_path)

        # Initialize source if it doesn't exist
        if source_key not in self._in_memory_store:
            self._in_memory_store[source_key] = {}

        local_data = self._in_memory_store[source_key]

        # Check for duplicate entry
        if entry_id in local_data:
            if not ignore_duplicate and self.duplicate_entry_behavior == "error":
                raise ValueError(
                    f"Entry '{entry_id}' already exists in {source_key}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )

        # Store the record
        local_data[entry_id] = arrow_data

        action = "Updated" if entry_id in local_data else "Added"
        logger.debug(f"{action} record {entry_id} in {source_key}")
        return arrow_data

    def load_existing_record(self, source_path: tuple[str, ...]):
        source_key = self._get_source_key(source_path)
        if self.base_path is not None and source_key not in self._in_memory_store:
            self.load_from_parquet(self.base_path, source_path)

    def get_record(
        self, source_path: tuple[str, ...], entry_id: str
    ) -> pa.Table | None:
        """Get a specific record."""
        self.load_existing_record(source_path)
        source_key = self._get_source_key(source_path)
        local_data = self._in_memory_store.get(source_key, {})
        return local_data.get(entry_id)

    def get_all_records(
        self, source_path: tuple[str, ...], add_entry_id_column: bool | str = False
    ) -> pa.Table | None:
        """Retrieve all records for a given source as a single table."""
        self.load_existing_record(source_path)
        source_key = self._get_source_key(source_path)
        local_data = self._in_memory_store.get(source_key, {})

        if not local_data:
            return None

        tables_with_keys = []
        for key, table in local_data.items():
            # Add entry_id column to each table
            key_array = pa.array([key] * len(table), type=pa.large_string())
            table_with_key = table.add_column(0, "__entry_id", key_array)
            tables_with_keys.append(table_with_key)

        # Concatenate all tables
        if tables_with_keys:
            combined_table = pa.concat_tables(tables_with_keys)
            if not add_entry_id_column:
                combined_table = combined_table.drop(columns=["__entry_id"])
            return combined_table
        return None

    def get_all_records_as_polars(
        self, source_path: tuple[str, ...]
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a single Polars LazyFrame."""
        all_records = self.get_all_records(source_path)
        if all_records is None:
            return None
        return pl.LazyFrame(all_records)

    def get_records_by_ids(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve records by entry IDs as a single table.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Arrow table containing all found records, or None if no records found
        """
        # Convert input to list of strings for consistency
        if isinstance(entry_ids, list):
            if not entry_ids:
                return None
            entry_ids_list = entry_ids
        elif isinstance(entry_ids, pl.Series):
            if len(entry_ids) == 0:
                return None
            entry_ids_list = entry_ids.to_list()
        elif isinstance(entry_ids, pa.Array):
            if len(entry_ids) == 0:
                return None
            entry_ids_list = entry_ids.to_pylist()
        else:
            raise TypeError(
                f"entry_ids must be list[str], pl.Series, or pa.Array, got {type(entry_ids)}"
            )

        self.load_existing_record(source_path)

        source_key = self._get_source_key(source_path)
        local_data = self._in_memory_store.get(source_key, {})

        if not local_data:
            return None

        # Collect matching tables
        found_tables = []
        found_entry_ids = []

        if preserve_input_order:
            # Preserve input order, include nulls for missing entries
            first_table_schema = None

            for entry_id in entry_ids_list:
                if entry_id in local_data:
                    table = local_data[entry_id]
                    # Add entry_id column
                    key_array = pa.array([entry_id] * len(table), type=pa.string())
                    table_with_key = table.add_column(0, "__entry_id", key_array)
                    found_tables.append(table_with_key)
                    found_entry_ids.append(entry_id)

                    # Store schema for creating null rows
                    if first_table_schema is None:
                        first_table_schema = table_with_key.schema
                else:
                    # Create a null row with the same schema as other tables
                    if first_table_schema is not None:
                        # Create null row
                        null_data = {}
                        for field in first_table_schema:
                            if field.name == "__entry_id":
                                null_data[field.name] = pa.array(
                                    [entry_id], type=field.type
                                )
                            else:
                                # Create null array with proper type
                                null_array = pa.array([None], type=field.type)
                                null_data[field.name] = null_array

                        null_table = pa.table(null_data, schema=first_table_schema)
                        found_tables.append(null_table)
                        found_entry_ids.append(entry_id)
        else:
            # Storage order (faster) - only include existing entries
            for entry_id in entry_ids_list:
                if entry_id in local_data:
                    table = local_data[entry_id]
                    # Add entry_id column
                    key_array = pa.array([entry_id] * len(table), type=pa.string())
                    table_with_key = table.add_column(0, "__entry_id", key_array)
                    found_tables.append(table_with_key)
                    found_entry_ids.append(entry_id)

        if not found_tables:
            return None

        # Concatenate all found tables
        if len(found_tables) == 1:
            combined_table = found_tables[0]
        else:
            combined_table = pa.concat_tables(found_tables)

        # Handle entry_id column based on add_entry_id_column parameter
        if add_entry_id_column is False:
            # Remove the __entry_id column
            column_names = combined_table.column_names
            if "__entry_id" in column_names:
                indices_to_keep = [
                    i for i, name in enumerate(column_names) if name != "__entry_id"
                ]
                combined_table = combined_table.select(indices_to_keep)
        elif isinstance(add_entry_id_column, str):
            # Rename __entry_id to custom name
            schema = combined_table.schema
            new_names = [
                add_entry_id_column if name == "__entry_id" else name
                for name in schema.names
            ]
            combined_table = combined_table.rename_columns(new_names)
        # If add_entry_id_column is True, keep __entry_id as is

        return combined_table

    def get_records_by_ids_as_polars(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        """
        Retrieve records by entry IDs as a single Polars LazyFrame.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Polars LazyFrame containing all found records, or None if no records found
        """
        # Get Arrow result and convert to Polars
        arrow_result = self.get_records_by_ids(
            source_path, entry_ids, add_entry_id_column, preserve_input_order
        )

        if arrow_result is None:
            return None

        # Convert to Polars LazyFrame
        return pl.LazyFrame(arrow_result)

    def save_to_parquet(self, base_path: str | Path) -> None:
        """
        Save all data to Parquet files in a directory structure.

        Directory structure: base_path/source_name/source_id/data.parquet

        Args:
            base_path: Base directory path where to save the Parquet files
        """
        base_path = Path(base_path)
        base_path.mkdir(parents=True, exist_ok=True)

        saved_count = 0

        for source_id, local_data in self._in_memory_store.items():
            if not local_data:
                continue

            # Create directory structure
            source_dir = base_path / source_id
            source_dir.mkdir(parents=True, exist_ok=True)

            # Combine all tables for this source with entry_id column
            tables_with_keys = []
            for entry_id, table in local_data.items():
                # Add entry_id column to each table
                key_array = pa.array([entry_id] * len(table), type=pa.string())
                table_with_key = table.add_column(0, "__entry_id", key_array)
                tables_with_keys.append(table_with_key)

            # Concatenate all tables
            if tables_with_keys:
                combined_table = pa.concat_tables(tables_with_keys)

                # Save as Parquet file
                # TODO: perform safe "atomic" write
                parquet_path = source_dir / "data.parquet"
                import pyarrow.parquet as pq

                pq.write_table(combined_table, parquet_path)

                saved_count += 1
                logger.debug(
                    f"Saved {len(combined_table)} records for {source_id} to {parquet_path}"
                )

        logger.info(f"Saved {saved_count} sources to Parquet files in {base_path}")

    def load_from_parquet(
        self, base_path: str | Path, source_path: tuple[str, ...]
    ) -> None:
        """
        Load data from Parquet files with the expected directory structure.

        Expected structure: base_path/source_name/source_id/data.parquet

        Args:
            base_path: Base directory path containing the Parquet files
        """

        source_key = self._get_source_key(source_path)
        target_path = Path(base_path) / source_key

        if not target_path.exists():
            logger.info(f"Base path {base_path} does not exist")
            return

        loaded_count = 0

        # Look for Parquet files in this directory
        parquet_files = list(target_path.glob("*.parquet"))
        if not parquet_files:
            logger.debug(f"No Parquet files found in {target_path}")
            return

        # Load all Parquet files and combine them
        all_records = []

        for parquet_file in parquet_files:
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(parquet_file)

                # Validate that __entry_id column exists
                if "__entry_id" not in table.column_names:
                    logger.warning(
                        f"Parquet file {parquet_file} missing __entry_id column, skipping"
                    )
                    continue

                all_records.append(table)
                logger.debug(f"Loaded {len(table)} records from {parquet_file}")

            except Exception as e:
                logger.error(f"Failed to load Parquet file {parquet_file}: {e}")
                continue

        # Process all records for this source
        if all_records:
            # Combine all tables
            if len(all_records) == 1:
                combined_table = all_records[0]
            else:
                combined_table = pa.concat_tables(all_records)

            # Split back into individual records by entry_id
            local_data = {}
            entry_ids = combined_table.column("__entry_id").to_pylist()

            # Group records by entry_id
            entry_id_groups = {}
            for i, entry_id in enumerate(entry_ids):
                if entry_id not in entry_id_groups:
                    entry_id_groups[entry_id] = []
                entry_id_groups[entry_id].append(i)

            # Extract each entry_id's records
            for entry_id, indices in entry_id_groups.items():
                # Take rows for this entry_id and remove __entry_id column
                entry_table = combined_table.take(indices)

                # Remove __entry_id column
                column_names = entry_table.column_names
                if "__entry_id" in column_names:
                    indices_to_keep = [
                        i for i, name in enumerate(column_names) if name != "__entry_id"
                    ]
                    entry_table = entry_table.select(indices_to_keep)

                local_data[entry_id] = entry_table

            self._in_memory_store[source_key] = local_data
            loaded_count += 1

            record_count = len(combined_table)
            unique_entries = len(entry_id_groups)
            logger.info(
                f"Loaded {record_count} records ({unique_entries} unique entries) for {source_key}"
            )

    def flush(self):
        """
        Flush all in-memory data to Parquet files in the base path.
        This will overwrite existing files.
        """
        if self.base_path is None:
            logger.warning("Base path is not set, cannot flush data")
            return

        logger.info(f"Flushing data to Parquet files in {self.base_path}")
        self.save_to_parquet(self.base_path)


@dataclass
class RecordMetadata:
    """Metadata for a stored record."""

    source_name: str
    source_id: str
    entry_id: str
    created_at: datetime
    updated_at: datetime
    schema_hash: str
    parquet_path: str | None = None  # Path to the specific partition


class SourceCache:
    """Cache for a specific source_name/source_id combination."""

    def __init__(
        self,
        source_name: str,
        source_id: str,
        base_path: Path,
        partition_prefix_length: int = 2,
    ):
        self.source_name = source_name
        self.source_id = source_id
        self.base_path = base_path
        self.source_dir = base_path / source_name / source_id
        self.partition_prefix_length = partition_prefix_length

        # In-memory data - only for this source
        self._memory_table: pl.DataFrame | None = None
        self._loaded = False
        self._dirty = False
        self._last_access = datetime.now()

        # Track which entries are in memory vs on disk
        self._memory_entries: set[str] = set()
        self._disk_entries: set[str] = set()

        # Track which partitions are dirty (need to be rewritten)
        self._dirty_partitions: set[str] = set()

        self._lock = threading.RLock()

    def _get_partition_key(self, entry_id: str) -> str:
        """Get the partition key for an entry_id."""
        if len(entry_id) < self.partition_prefix_length:
            return entry_id.ljust(self.partition_prefix_length, "0")
        return entry_id[: self.partition_prefix_length]

    def _get_partition_path(self, entry_id: str) -> Path:
        """Get the partition directory for an entry_id."""
        partition_key = self._get_partition_key(entry_id)
        # Use prefix_ instead of entry_id= to avoid Hive partitioning issues
        return self.source_dir / f"prefix_{partition_key}"

    def _get_partition_parquet_path(self, entry_id: str) -> Path:
        """Get the Parquet file path for a partition."""
        partition_dir = self._get_partition_path(entry_id)
        partition_key = self._get_partition_key(entry_id)
        return partition_dir / f"partition_{partition_key}.parquet"

    def _load_from_disk_lazy(self) -> None:
        """Lazily load data from disk only when first accessed."""
        if self._loaded:
            return

        with self._lock:
            if self._loaded:  # Double-check after acquiring lock
                return

            logger.debug(f"Lazy loading {self.source_name}/{self.source_id}")

            all_tables = []

            if self.source_dir.exists():
                # Scan all partition directories
                for partition_dir in self.source_dir.iterdir():
                    if not partition_dir.is_dir() or not (
                        partition_dir.name.startswith("entry_id=")
                        or partition_dir.name.startswith("prefix_")
                    ):
                        continue

                    # Load the partition Parquet file (one per partition)
                    if partition_dir.name.startswith("entry_id="):
                        partition_key = partition_dir.name.split("=")[1]
                    else:  # prefix_XX format
                        partition_key = partition_dir.name.split("_")[1]

                    parquet_file = partition_dir / f"partition_{partition_key}.parquet"

                    if parquet_file.exists():
                        try:
                            table = pq.read_table(parquet_file)
                            if len(table) > 0:
                                polars_df = pl.from_arrow(table)
                                all_tables.append(polars_df)

                                logger.debug(
                                    f"Loaded partition {parquet_file}: {len(table)} rows, {len(table.columns)} columns"
                                )
                                logger.debug(f"  Columns: {table.column_names}")

                                # Track disk entries from this partition
                                if "__entry_id" in table.column_names:
                                    entry_ids = set(
                                        table.column("__entry_id").to_pylist()
                                    )
                                    self._disk_entries.update(entry_ids)

                        except Exception as e:
                            logger.error(f"Failed to load {parquet_file}: {e}")

            # Combine all tables
            if all_tables:
                self._memory_table = pl.concat(all_tables)
                self._memory_entries = self._disk_entries.copy()
                logger.debug(
                    f"Combined loaded data: {len(self._memory_table)} rows, {len(self._memory_table.columns)} columns"
                )
                logger.debug(f"  Final columns: {self._memory_table.columns}")

            self._loaded = True
            self._last_access = datetime.now()

    def add_entry(
        self,
        entry_id: str,
        table_with_metadata: pa.Table,
        allow_overwrite: bool = False,
    ) -> None:
        """Add an entry to this source cache."""
        with self._lock:
            self._load_from_disk_lazy()  # Ensure we're loaded

            # Check if entry already exists
            entry_exists = (
                entry_id in self._memory_entries or entry_id in self._disk_entries
            )

            if entry_exists and not allow_overwrite:
                raise ValueError(
                    f"Entry {entry_id} already exists in {self.source_name}/{self.source_id}"
                )

            # We know this returns DataFrame since we're passing a Table
            polars_table = cast(pl.DataFrame, pl.from_arrow(table_with_metadata))

            if self._memory_table is None:
                self._memory_table = polars_table
            else:
                # Remove existing entry if it exists (for overwrite case)
                if entry_id in self._memory_entries:
                    mask = self._memory_table["__entry_id"] != entry_id
                    self._memory_table = self._memory_table.filter(mask)
                    logger.debug(f"Removed existing entry {entry_id} for overwrite")

                # Debug schema mismatch
                existing_cols = self._memory_table.columns
                new_cols = polars_table.columns

                if len(existing_cols) != len(new_cols):
                    logger.error(f"Schema mismatch for entry {entry_id}:")
                    logger.error(
                        f"  Existing columns ({len(existing_cols)}): {existing_cols}"
                    )
                    logger.error(f"  New columns ({len(new_cols)}): {new_cols}")
                    logger.error(
                        f"  Missing in new: {set(existing_cols) - set(new_cols)}"
                    )
                    logger.error(
                        f"  Extra in new: {set(new_cols) - set(existing_cols)}"
                    )

                    raise ValueError(
                        f"Schema mismatch: existing table has {len(existing_cols)} columns, "
                        f"new table has {len(new_cols)} columns"
                    )

                # Ensure column order matches
                if existing_cols != new_cols:
                    logger.debug("Reordering columns to match existing schema")
                    polars_table = polars_table.select(existing_cols)

                # Add new entry
                self._memory_table = pl.concat([self._memory_table, polars_table])

            self._memory_entries.add(entry_id)
            self._dirty = True

            # Mark the partition as dirty
            partition_key = self._get_partition_key(entry_id)
            self._dirty_partitions.add(partition_key)

            self._last_access = datetime.now()

            if entry_exists:
                logger.info(f"Overwrote existing entry {entry_id}")
            else:
                logger.debug(f"Added new entry {entry_id}")

    def get_entry(self, entry_id: str) -> pa.Table | None:
        """Get a specific entry."""
        with self._lock:
            self._load_from_disk_lazy()

            if self._memory_table is None:
                return None

            mask = self._memory_table["__entry_id"] == entry_id
            filtered = self._memory_table.filter(mask)

            if len(filtered) == 0:
                return None

            self._last_access = datetime.now()
            return filtered.to_arrow()

    def get_all_entries(self) -> pa.Table | None:
        """Get all entries for this source."""
        with self._lock:
            self._load_from_disk_lazy()

            if self._memory_table is None:
                return None

            self._last_access = datetime.now()
            return self._memory_table.to_arrow()

    def get_all_entries_as_polars(self) -> pl.LazyFrame | None:
        """Get all entries as a Polars LazyFrame."""
        with self._lock:
            self._load_from_disk_lazy()

            if self._memory_table is None:
                return None

            self._last_access = datetime.now()
            return self._memory_table.lazy()

    def sync_to_disk(self) -> None:
        """Sync dirty partitions to disk using efficient Parquet files."""
        with self._lock:
            if not self._dirty or self._memory_table is None:
                return

            logger.debug(f"Syncing {self.source_name}/{self.source_id} to disk")

            # Only sync dirty partitions
            for partition_key in self._dirty_partitions:
                try:
                    # Get all entries for this partition
                    partition_mask = (
                        self._memory_table["__entry_id"].str.slice(
                            0, self.partition_prefix_length
                        )
                        == partition_key
                    )
                    partition_data = self._memory_table.filter(partition_mask)

                    if len(partition_data) == 0:
                        continue

                    logger.debug(f"Syncing partition {partition_key}:")
                    logger.debug(f"  Rows: {len(partition_data)}")
                    logger.debug(f"  Columns: {partition_data.columns}")
                    logger.debug(
                        f"  Sample __entry_id values: {partition_data['__entry_id'].head(3).to_list()}"
                    )

                    # Ensure partition directory exists
                    partition_dir = self.source_dir / f"prefix_{partition_key}"
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    # Write entire partition to single Parquet file
                    partition_path = (
                        partition_dir / f"partition_{partition_key}.parquet"
                    )
                    arrow_table = partition_data.to_arrow()

                    logger.debug(
                        f"  Arrow table columns before write: {arrow_table.column_names}"
                    )
                    logger.debug(f"  Arrow table shape: {arrow_table.shape}")

                    pq.write_table(arrow_table, partition_path)

                    # Verify what was written
                    verification_table = pq.read_table(partition_path)
                    logger.debug(
                        f"  Verification - columns after write: {verification_table.column_names}"
                    )
                    logger.debug(f"  Verification - shape: {verification_table.shape}")

                    entry_count = len(set(partition_data["__entry_id"].to_list()))
                    logger.debug(
                        f"Wrote partition {partition_key} with {entry_count} entries ({len(partition_data)} rows)"
                    )

                except Exception as e:
                    logger.error(f"Failed to write partition {partition_key}: {e}")
                    import traceback

                    logger.error(f"Traceback: {traceback.format_exc()}")

            # Clear dirty markers
            self._dirty_partitions.clear()
            self._dirty = False

    def is_loaded(self) -> bool:
        """Check if this cache is loaded in memory."""
        return self._loaded

    def get_last_access(self) -> datetime:
        """Get the last access time."""
        return self._last_access

    def unload(self) -> None:
        """Unload from memory (after syncing if dirty)."""
        with self._lock:
            if self._dirty:
                self.sync_to_disk()

            self._memory_table = None
            self._loaded = False
            self._memory_entries.clear()
            # Keep _disk_entries for reference

    def entry_exists(self, entry_id: str) -> bool:
        """Check if an entry exists (in memory or on disk)."""
        with self._lock:
            self._load_from_disk_lazy()
            return entry_id in self._memory_entries or entry_id in self._disk_entries

    def list_entries(self) -> set[str]:
        """List all entry IDs in this source."""
        with self._lock:
            self._load_from_disk_lazy()
            return self._memory_entries | self._disk_entries

    def get_stats(self) -> dict[str, Any]:
        """Get statistics for this cache."""
        with self._lock:
            return {
                "source_name": self.source_name,
                "source_id": self.source_id,
                "loaded": self._loaded,
                "dirty": self._dirty,
                "memory_entries": len(self._memory_entries),
                "disk_entries": len(self._disk_entries),
                "memory_rows": len(self._memory_table)
                if self._memory_table is not None
                else 0,
                "last_access": self._last_access.isoformat(),
            }


class ParquetArrowDataStore:
    """
    Lazy-loading, append-only Arrow data store with entry_id partitioning.

    Features:
    - Lazy loading: Only loads source data when first accessed
    - Separate memory management per source_name/source_id
    - Entry_id partitioning: Multiple entries per Parquet file based on prefix
    - Configurable duplicate entry_id handling (error or overwrite)
    - Automatic cache eviction for memory management
    - Single-row constraint: Each record must contain exactly one row
    """

    _system_columns = [
        "__source_name",
        "__source_id",
        "__entry_id",
        "__created_at",
        "__updated_at",
        "__schema_hash",
    ]

    def __init__(
        self,
        base_path: str | Path,
        sync_interval_seconds: int = 300,  # 5 minutes default
        auto_sync: bool = True,
        max_loaded_sources: int = 100,
        cache_eviction_hours: int = 2,
        duplicate_entry_behavior: str = "error",
        partition_prefix_length: int = 2,
    ):
        """
        Initialize the ParquetArrowDataStore.

        Args:
            base_path: Directory path for storing Parquet files
            sync_interval_seconds: How often to sync dirty caches to disk
            auto_sync: Whether to automatically sync on a timer
            max_loaded_sources: Maximum number of source caches to keep in memory
            cache_eviction_hours: Hours of inactivity before evicting from memory
            duplicate_entry_behavior: How to handle duplicate entry_ids:
                - 'error': Raise ValueError when entry_id already exists
                - 'overwrite': Replace existing entry with new data
            partition_prefix_length: Number of characters from entry_id to use for partitioning (default 2)
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.sync_interval = sync_interval_seconds
        self.auto_sync = auto_sync
        self.max_loaded_sources = max_loaded_sources
        self.cache_eviction_hours = cache_eviction_hours
        self.partition_prefix_length = max(
            1, min(8, partition_prefix_length)
        )  # Clamp between 1-8

        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")
        self.duplicate_entry_behavior = duplicate_entry_behavior

        # Cache management
        self._source_caches: dict[str, SourceCache] = {}  # key: "source_name:source_id"
        self._global_lock = threading.RLock()

        # Record metadata (always in memory for fast lookups)
        self._record_metadata: dict[str, RecordMetadata] = {}
        self._load_metadata_index()

        # Sync management
        self._sync_timer: threading.Timer | None = None
        self._shutdown = False

        # Start auto-sync and cleanup if enabled
        if self.auto_sync:
            self._start_sync_timer()

        logger.info(f"Initialized lazy ParquetArrowDataStore at {base_path}")

    def _get_source_key(self, source_name: str, source_id: str) -> str:
        """Generate key for source cache."""
        return f"{source_name}:{source_id}"

    def _get_record_key(self, source_name: str, source_id: str, entry_id: str) -> str:
        """Generate unique key for a record."""
        return f"{source_name}:{source_id}:{entry_id}"

    def _load_metadata_index(self) -> None:
        """Load metadata index from disk (lightweight - just file paths and timestamps)."""
        logger.info("Loading metadata index...")

        if not self.base_path.exists():
            return

        for source_name_dir in self.base_path.iterdir():
            if not source_name_dir.is_dir():
                continue

            source_name = source_name_dir.name

            for source_id_dir in source_name_dir.iterdir():
                if not source_id_dir.is_dir():
                    continue

                source_id = source_id_dir.name

                # Scan partition directories for parquet files
                for partition_dir in source_id_dir.iterdir():
                    if not partition_dir.is_dir() or not (
                        partition_dir.name.startswith("entry_id=")
                        or partition_dir.name.startswith("prefix_")
                    ):
                        continue

                    for parquet_file in partition_dir.glob("partition_*.parquet"):
                        try:
                            # Read the parquet file to extract entry IDs
                            table = pq.read_table(parquet_file)
                            if "__entry_id" in table.column_names:
                                entry_ids = set(table.column("__entry_id").to_pylist())

                                # Get file stats
                                stat = parquet_file.stat()
                                created_at = datetime.fromtimestamp(stat.st_ctime)
                                updated_at = datetime.fromtimestamp(stat.st_mtime)

                                for entry_id in entry_ids:
                                    record_key = self._get_record_key(
                                        source_name, source_id, entry_id
                                    )
                                    self._record_metadata[record_key] = RecordMetadata(
                                        source_name=source_name,
                                        source_id=source_id,
                                        entry_id=entry_id,
                                        created_at=created_at,
                                        updated_at=updated_at,
                                        schema_hash="unknown",  # Will be computed if needed
                                        parquet_path=str(parquet_file),
                                    )
                        except Exception as e:
                            logger.error(
                                f"Failed to read metadata from {parquet_file}: {e}"
                            )

        logger.info(f"Loaded metadata for {len(self._record_metadata)} records")

    def _get_or_create_source_cache(
        self, source_name: str, source_id: str
    ) -> SourceCache:
        """Get or create a source cache, handling eviction if needed."""
        source_key = self._get_source_key(source_name, source_id)

        with self._global_lock:
            if source_key not in self._source_caches:
                # Check if we need to evict old caches
                if len(self._source_caches) >= self.max_loaded_sources:
                    self._evict_old_caches()

                # Create new cache with partition configuration
                self._source_caches[source_key] = SourceCache(
                    source_name, source_id, self.base_path, self.partition_prefix_length
                )
                logger.debug(f"Created cache for {source_key}")

            return self._source_caches[source_key]

    def _evict_old_caches(self) -> None:
        """Evict old caches based on last access time."""
        cutoff_time = datetime.now() - timedelta(hours=self.cache_eviction_hours)

        to_evict = []
        for source_key, cache in self._source_caches.items():
            if cache.get_last_access() < cutoff_time:
                to_evict.append(source_key)

        for source_key in to_evict:
            cache = self._source_caches.pop(source_key)
            cache.unload()  # This will sync if dirty
            logger.debug(f"Evicted cache for {source_key}")

    def _compute_schema_hash(self, table: pa.Table) -> str:
        """Compute a hash of the table schema."""
        import hashlib

        schema_str = str(table.schema)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def _add_system_columns(
        self, table: pa.Table, metadata: RecordMetadata
    ) -> pa.Table:
        """Add system columns to track record metadata."""
        # Keep all system columns for self-describing data
        # Use large_string for all string columns
        large_string_type = pa.large_string()

        system_columns = [
            (
                "__source_name",
                pa.array([metadata.source_name] * len(table), type=large_string_type),
            ),
            (
                "__source_id",
                pa.array([metadata.source_id] * len(table), type=large_string_type),
            ),
            (
                "__entry_id",
                pa.array([metadata.entry_id] * len(table), type=large_string_type),
            ),
            ("__created_at", pa.array([metadata.created_at] * len(table))),
            ("__updated_at", pa.array([metadata.updated_at] * len(table))),
            (
                "__schema_hash",
                pa.array([metadata.schema_hash] * len(table), type=large_string_type),
            ),
        ]

        # Combine user columns + system columns in consistent order
        new_columns = list(table.columns) + [col[1] for col in system_columns]
        new_names = table.column_names + [col[0] for col in system_columns]

        result = pa.table(new_columns, names=new_names)
        logger.debug(
            f"Added system columns: {len(table.columns)} -> {len(result.columns)} columns"
        )
        return result

    def _remove_system_columns(self, table: pa.Table) -> pa.Table:
        """Remove system columns to get original user data."""
        return table.drop(self._system_columns)

    def add_record(
        self, source_name: str, source_id: str, entry_id: str, arrow_data: pa.Table
    ) -> pa.Table:
        """
        Add or update a record (append-only operation).

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_id: Unique identifier for this record (typically 32-char hash)
            arrow_data: The Arrow table data to store (MUST contain exactly 1 row)

        Returns:
            The original arrow_data table

        Raises:
            ValueError: If entry_id already exists and duplicate_entry_behavior is 'error'
            ValueError: If arrow_data contains more than 1 row
            ValueError: If arrow_data schema doesn't match existing data for this source
        """
        # normalize arrow_data to conform to polars string. TODO: consider a clearner approach
        arrow_data = pl.DataFrame(arrow_data).to_arrow()

        # CRITICAL: Enforce single-row constraint
        if len(arrow_data) != 1:
            raise ValueError(
                f"Each record must contain exactly 1 row, got {len(arrow_data)} rows. "
                f"This constraint ensures that for each source_name/source_id combination, "
                f"there is only one valid entry per entry_id."
            )

        # Validate entry_id format (assuming 8+ char identifier)
        if not entry_id or len(entry_id) < 8:
            raise ValueError(
                f"entry_id must be at least 8 characters long, got: '{entry_id}'"
            )

        # Check if this source already has data and validate schema compatibility
        cache = self._get_or_create_source_cache(source_name, source_id)

        # Load existing data to check schema compatibility
        cache._load_from_disk_lazy()

        if cache._memory_table is not None:
            # Extract user columns from existing data (remove system columns)
            existing_arrow = cache._memory_table.to_arrow()
            existing_user_data = self._remove_system_columns(existing_arrow)

            # Check if schemas match
            existing_schema = existing_user_data.schema
            new_schema = arrow_data.schema

            if not existing_schema.equals(new_schema):
                existing_cols = existing_user_data.column_names
                new_cols = arrow_data.column_names

                logger.error(f"Schema mismatch for {source_name}/{source_id}:")
                logger.error(f"  Existing user columns: {existing_cols}")
                logger.error(f"  New user columns: {new_cols}")
                logger.error(f"  Missing in new: {set(existing_cols) - set(new_cols)}")
                logger.error(f"  Extra in new: {set(new_cols) - set(existing_cols)}")

                raise ValueError(
                    f"Schema mismatch for {source_name}/{source_id}. "
                    f"Existing data has columns {existing_cols}, "
                    f"but new data has columns {new_cols}. "
                    f"All records in a source must have the same schema."
                )

        now = datetime.now()
        record_key = self._get_record_key(source_name, source_id, entry_id)

        # Check for existing entry
        existing_metadata = self._record_metadata.get(record_key)
        entry_exists = existing_metadata is not None

        if entry_exists and self.duplicate_entry_behavior == "error":
            raise DuplicateError(
                f"Entry '{entry_id}' already exists in {source_name}/{source_id}. "
                f"Use duplicate_entry_behavior='overwrite' to allow updates."
            )

        # Create/update metadata
        schema_hash = self._compute_schema_hash(arrow_data)
        metadata = RecordMetadata(
            source_name=source_name,
            source_id=source_id,
            entry_id=entry_id,
            created_at=existing_metadata.created_at if existing_metadata else now,
            updated_at=now,
            schema_hash=schema_hash,
        )

        # Add system columns
        table_with_metadata = self._add_system_columns(arrow_data, metadata)

        # Get or create source cache and add entry
        allow_overwrite = self.duplicate_entry_behavior == "overwrite"

        try:
            cache.add_entry(entry_id, table_with_metadata, allow_overwrite)
        except ValueError as e:
            # Re-raise with more context
            raise ValueError(f"Failed to add record: {e}")

        # Update metadata
        self._record_metadata[record_key] = metadata

        action = "Updated" if entry_exists else "Added"
        logger.info(f"{action} record {record_key} with {len(arrow_data)} rows")
        return arrow_data

    def get_record(
        self, source_name: str, source_id: str, entry_id: str
    ) -> pa.Table | None:
        """Retrieve a specific record."""
        record_key = self._get_record_key(source_name, source_id, entry_id)

        if record_key not in self._record_metadata:
            return None

        cache = self._get_or_create_source_cache(source_name, source_id)
        table = cache.get_entry(entry_id)

        if table is None:
            return None

        return self._remove_system_columns(table)

    def get_all_records(
        self, source_name: str, source_id: str, _keep_system_columns: bool = False
    ) -> pa.Table | None:
        """Retrieve all records for a given source as a single Arrow table."""
        cache = self._get_or_create_source_cache(source_name, source_id)
        table = cache.get_all_entries()

        if table is None:
            return None

        if _keep_system_columns:
            return table
        return self._remove_system_columns(table)

    def get_all_records_as_polars(
        self, source_name: str, source_id: str, _keep_system_columns: bool = False
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a Polars LazyFrame."""
        cache = self._get_or_create_source_cache(source_name, source_id)
        lazy_frame = cache.get_all_entries_as_polars()

        if lazy_frame is None:
            return None

        if _keep_system_columns:
            return lazy_frame

        return lazy_frame.drop(self._system_columns)

    def get_records_by_ids(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve multiple records by their entry_ids as a single Arrow table.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Arrow table containing all found records, or None if no records found
            When preserve_input_order=True, table length equals input length
            When preserve_input_order=False, records are in storage order
        """
        # Get Polars result using the Polars method
        polars_result = self.get_records_by_ids_as_polars(
            source_name, source_id, entry_ids, add_entry_id_column, preserve_input_order
        )

        if polars_result is None:
            return None

        # Convert to Arrow table
        return polars_result.collect().to_arrow()

    def get_records_by_ids_as_polars(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        """
        Retrieve multiple records by their entry_ids as a Polars LazyFrame.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Polars LazyFrame containing all found records, or None if no records found
            When preserve_input_order=True, frame length equals input length
            When preserve_input_order=False, records are in storage order (existing behavior)
        """
        # Convert input to Polars Series
        if isinstance(entry_ids, list):
            if not entry_ids:
                return None
            entry_ids_series = pl.Series("entry_id", entry_ids)
        elif isinstance(entry_ids, pl.Series):
            if len(entry_ids) == 0:
                return None
            entry_ids_series = entry_ids
        elif isinstance(entry_ids, pa.Array):
            if len(entry_ids) == 0:
                return None
            entry_ids_series = pl.Series(
                "entry_id", entry_ids
            )  # Direct from Arrow array
        else:
            raise TypeError(
                f"entry_ids must be list[str], pl.Series, or pa.Array, got {type(entry_ids)}"
            )

        cache = self._get_or_create_source_cache(source_name, source_id)
        lazy_frame = cache.get_all_entries_as_polars()

        if lazy_frame is None:
            return None

        # Define system columns that are always excluded (except optionally __entry_id)
        system_cols = [
            "__source_name",
            "__source_id",
            "__created_at",
            "__updated_at",
            "__schema_hash",
        ]

        # Add __entry_id to system columns if we don't want it in the result
        if add_entry_id_column is False:
            system_cols.append("__entry_id")

        # Handle input order preservation vs filtering
        if preserve_input_order:
            # Create ordered DataFrame with input IDs and join to preserve order with nulls
            ordered_df = pl.DataFrame({"__entry_id": entry_ids_series}).lazy()
            # Join with all data to get results in input order with nulls for missing
            result_frame = ordered_df.join(lazy_frame, on="__entry_id", how="left")
        else:
            # Standard filtering approach for storage order -- should be faster in general
            result_frame = lazy_frame.filter(
                pl.col("__entry_id").is_in(entry_ids_series)
            )

        # Apply column selection (same for both paths)
        result_frame = result_frame.drop(system_cols)

        # Rename __entry_id column if custom name provided
        if isinstance(add_entry_id_column, str):
            result_frame = result_frame.rename({"__entry_id": add_entry_id_column})

        return result_frame

    def _sync_all_dirty_caches(self) -> None:
        """Sync all dirty caches to disk."""
        with self._global_lock:
            dirty_count = 0
            for cache in self._source_caches.values():
                if cache._dirty:
                    cache.sync_to_disk()
                    dirty_count += 1

            if dirty_count > 0:
                logger.info(f"Synced {dirty_count} dirty caches to disk")

    def _start_sync_timer(self) -> None:
        """Start the automatic sync timer."""
        if self._shutdown:
            return

        self._sync_timer = threading.Timer(
            self.sync_interval, self._sync_and_reschedule
        )
        self._sync_timer.daemon = True
        self._sync_timer.start()

    def _sync_and_reschedule(self) -> None:
        """Sync dirty caches and reschedule."""
        try:
            self._sync_all_dirty_caches()
            self._evict_old_caches()
        except Exception as e:
            logger.error(f"Auto-sync failed: {e}")
        finally:
            if not self._shutdown:
                self._start_sync_timer()

    def force_sync(self) -> None:
        """Manually trigger a sync of all dirty caches."""
        self._sync_all_dirty_caches()

    def entry_exists(self, source_name: str, source_id: str, entry_id: str) -> bool:
        """Check if a specific entry exists."""
        record_key = self._get_record_key(source_name, source_id, entry_id)

        # Check metadata first (fast)
        if record_key in self._record_metadata:
            return True

        # If not in metadata, check if source cache knows about it
        source_key = self._get_source_key(source_name, source_id)
        if source_key in self._source_caches:
            cache = self._source_caches[source_key]
            return cache.entry_exists(entry_id)

        # Not loaded and not in metadata - doesn't exist
        return False

    def list_entries(self, source_name: str, source_id: str) -> set[str]:
        """List all entry IDs for a specific source."""
        cache = self._get_or_create_source_cache(source_name, source_id)
        return cache.list_entries()

    def list_sources(self) -> set[tuple[str, str]]:
        """List all (source_name, source_id) combinations."""
        sources = set()

        # From metadata
        for metadata in self._record_metadata.values():
            sources.add((metadata.source_name, metadata.source_id))

        return sources

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive statistics about the data store."""
        with self._global_lock:
            loaded_caches = len(self._source_caches)
            dirty_caches = sum(
                1 for cache in self._source_caches.values() if cache._dirty
            )

            cache_stats = [cache.get_stats() for cache in self._source_caches.values()]

            return {
                "total_records": len(self._record_metadata),
                "loaded_source_caches": loaded_caches,
                "dirty_caches": dirty_caches,
                "max_loaded_sources": self.max_loaded_sources,
                "sync_interval": self.sync_interval,
                "auto_sync": self.auto_sync,
                "cache_eviction_hours": self.cache_eviction_hours,
                "base_path": str(self.base_path),
                "duplicate_entry_behavior": self.duplicate_entry_behavior,
                "partition_prefix_length": self.partition_prefix_length,
                "cache_details": cache_stats,
            }

    def shutdown(self) -> None:
        """Shutdown the data store, ensuring all data is synced."""
        logger.info("Shutting down ParquetArrowDataStore...")
        self._shutdown = True

        if self._sync_timer:
            self._sync_timer.cancel()

        # Final sync of all caches
        self._sync_all_dirty_caches()

        logger.info("Shutdown complete")

    def __del__(self):
        """Ensure cleanup on destruction."""
        if not self._shutdown:
            self.shutdown()


# Example usage and testing
def demo_single_row_constraint():
    """Demonstrate the single-row constraint in the ParquetArrowDataStore."""
    import tempfile
    import random
    from datetime import timedelta

    def create_single_row_record(entry_id: str, value: float | None = None) -> pa.Table:
        """Create a single-row Arrow table."""
        if value is None:
            value = random.uniform(0, 100)

        return pa.table(
            {
                "entry_id": [entry_id],
                "timestamp": [datetime.now()],
                "value": [value],
                "category": [random.choice(["A", "B", "C"])],
            }
        )

    def create_multi_row_record(entry_id: str, num_rows: int = 3) -> pa.Table:
        """Create a multi-row Arrow table (should be rejected)."""
        return pa.table(
            {
                "entry_id": [entry_id] * num_rows,
                "timestamp": [
                    datetime.now() + timedelta(seconds=i) for i in range(num_rows)
                ],
                "value": [random.uniform(0, 100) for _ in range(num_rows)],
                "category": [random.choice(["A", "B", "C"]) for _ in range(num_rows)],
            }
        )

    print("Testing Single-Row Constraint...")

    with tempfile.TemporaryDirectory() as temp_dir:
        store = ParquetArrowDataStore(
            base_path=temp_dir,
            sync_interval_seconds=10,
            auto_sync=False,  # Manual sync for testing
            duplicate_entry_behavior="overwrite",
        )

        try:
            print("\n=== Testing Valid Single-Row Records ===")

            # Test 1: Add valid single-row records
            valid_entries = [
                "entry_001_abcdef1234567890abcdef1234567890",
                "entry_002_abcdef1234567890abcdef1234567890",
                "entry_003_abcdef1234567890abcdef1234567890",
            ]

            for i, entry_id in enumerate(valid_entries):
                data = create_single_row_record(entry_id, value=100.0 + i)
                store.add_record("experiments", "dataset_A", entry_id, data)
                print(
                    f"✓ Added single-row record {entry_id[:16]}... (value: {100.0 + i})"
                )

            print(f"\nTotal records stored: {len(store._record_metadata)}")

            print("\n=== Testing Invalid Multi-Row Records ===")

            # Test 2: Try to add multi-row record (should fail)
            invalid_entry = "entry_004_abcdef1234567890abcdef1234567890"
            try:
                invalid_data = create_multi_row_record(invalid_entry, num_rows=3)
                store.add_record(
                    "experiments", "dataset_A", invalid_entry, invalid_data
                )
                print("✗ ERROR: Multi-row record was accepted!")
            except ValueError as e:
                print(f"✓ Correctly rejected multi-row record: {str(e)[:80]}...")

            # Test 3: Try to add empty record (should fail)
            empty_entry = "entry_005_abcdef1234567890abcdef1234567890"
            try:
                empty_data = pa.table({"col1": pa.array([], type=pa.int64())})
                store.add_record("experiments", "dataset_A", empty_entry, empty_data)
                print("✗ ERROR: Empty record was accepted!")
            except ValueError as e:
                print(f"✓ Correctly rejected empty record: {str(e)[:80]}...")

            print("\n=== Testing Retrieval ===")

            # Test 4: Retrieve records
            retrieved = store.get_record("experiments", "dataset_A", valid_entries[0])
            if retrieved and len(retrieved) == 1:
                print(f"✓ Retrieved single record: {len(retrieved)} row")
                print(f"  Value: {retrieved.column('value')[0].as_py()}")
            else:
                print("✗ Failed to retrieve record or wrong size")

            # Test 5: Get all records
            all_records = store.get_all_records("experiments", "dataset_A")
            if all_records:
                print(f"✓ Retrieved all records: {len(all_records)} rows total")
                unique_entries = len(set(all_records.column("entry_id").to_pylist()))
                print(f"  Unique entries: {unique_entries}")

                # Verify each entry appears exactly once
                entry_counts = {}
                for entry_id in all_records.column("entry_id").to_pylist():
                    entry_counts[entry_id] = entry_counts.get(entry_id, 0) + 1

                all_single = all(count == 1 for count in entry_counts.values())
                if all_single:
                    print(
                        "✓ Each entry appears exactly once (single-row constraint maintained)"
                    )
                else:
                    print("✗ Some entries appear multiple times!")

            print("\n=== Testing Overwrite Behavior ===")

            # Test 6: Overwrite existing single-row record
            overwrite_data = create_single_row_record(valid_entries[0], value=999.0)
            store.add_record(
                "experiments", "dataset_A", valid_entries[0], overwrite_data
            )
            print("✓ Overwrote existing record")

            # Verify overwrite
            updated_record = store.get_record(
                "experiments", "dataset_A", valid_entries[0]
            )
            if updated_record and updated_record.column("value")[0].as_py() == 999.0:
                print(
                    f"✓ Overwrite successful: new value = {updated_record.column('value')[0].as_py()}"
                )

            # Sync and show final stats
            store.force_sync()
            stats = store.get_stats()
            print("\n=== Final Statistics ===")
            print(f"Total records: {stats['total_records']}")
            print(f"Loaded caches: {stats['loaded_source_caches']}")
            print(f"Dirty caches: {stats['dirty_caches']}")

        finally:
            store.shutdown()

    print("\n✓ Single-row constraint testing completed successfully!")


class InMemoryPolarsDataStore:
    """
    In-memory Arrow data store using Polars DataFrames for efficient storage and retrieval.
    This class provides the same interface as InMemoryArrowDataStore but uses Polars internally
    for better performance with large datasets and complex queries.

    Uses dict of Polars DataFrames for efficient storage and retrieval.
    Each DataFrame contains all records for a source with an __entry_id column.
    """

    def __init__(self, duplicate_entry_behavior: str = "error"):
        """
        Initialize the InMemoryPolarsDataStore.

        Args:
            duplicate_entry_behavior: How to handle duplicate entry_ids:
                - 'error': Raise ValueError when entry_id already exists
                - 'overwrite': Replace existing entry with new data
        """
        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")
        self.duplicate_entry_behavior = duplicate_entry_behavior

        # Store Polars DataFrames: {source_key: polars_dataframe}
        # Each DataFrame has an __entry_id column plus user data columns
        self._in_memory_store: dict[str, pl.DataFrame] = {}
        logger.info(
            f"Initialized InMemoryPolarsDataStore with duplicate_entry_behavior='{duplicate_entry_behavior}'"
        )

    def _get_source_key(self, source_name: str, source_id: str) -> str:
        """Generate key for source storage."""
        return f"{source_name}:{source_id}"

    def add_record(
        self,
        source_name: str,
        source_id: str,
        entry_id: str,
        arrow_data: pa.Table,
    ) -> pa.Table:
        """
        Add a record to the in-memory store.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_id: Unique identifier for this record
            arrow_data: The Arrow table data to store

        Returns:
            arrow_data equivalent to having loaded the corresponding entry that was just saved

        Raises:
            ValueError: If entry_id already exists and duplicate_entry_behavior is 'error'
        """
        source_key = self._get_source_key(source_name, source_id)

        # Convert Arrow table to Polars DataFrame and add entry_id column
        polars_data = cast(pl.DataFrame, pl.from_arrow(arrow_data))

        # Add __entry_id column
        polars_data = polars_data.with_columns(pl.lit(entry_id).alias("__entry_id"))

        # Check if source exists
        if source_key not in self._in_memory_store:
            # First record for this source
            self._in_memory_store[source_key] = polars_data
            logger.debug(f"Created new source {source_key} with entry {entry_id}")
        else:
            existing_df = self._in_memory_store[source_key]

            # Check for duplicate entry
            entry_exists = (
                existing_df.filter(pl.col("__entry_id") == entry_id).shape[0] > 0
            )

            if entry_exists:
                if self.duplicate_entry_behavior == "error":
                    raise ValueError(
                        f"Entry '{entry_id}' already exists in {source_name}/{source_id}. "
                        f"Use duplicate_entry_behavior='overwrite' to allow updates."
                    )
                else:  # validity of value is checked in constructor so it must be "ovewrite"
                    # Remove existing entry and add new one
                    existing_df = existing_df.filter(pl.col("__entry_id") != entry_id)
                    self._in_memory_store[source_key] = pl.concat(
                        [existing_df, polars_data]
                    )
                    logger.debug(f"Overwrote entry {entry_id} in {source_key}")
            else:
                # Append new entry
                try:
                    self._in_memory_store[source_key] = pl.concat(
                        [existing_df, polars_data]
                    )
                    logger.debug(f"Added entry {entry_id} to {source_key}")
                except Exception as e:
                    # Handle schema mismatch
                    existing_cols = set(existing_df.columns) - {"__entry_id"}
                    new_cols = set(polars_data.columns) - {"__entry_id"}

                    if existing_cols != new_cols:
                        raise ValueError(
                            f"Schema mismatch for {source_key}. "
                            f"Existing columns: {sorted(existing_cols)}, "
                            f"New columns: {sorted(new_cols)}"
                        ) from e
                    else:
                        raise e

        return arrow_data

    def get_record(
        self, source_name: str, source_id: str, entry_id: str
    ) -> pa.Table | None:
        """Get a specific record."""
        source_key = self._get_source_key(source_name, source_id)

        if source_key not in self._in_memory_store:
            return None

        df = self._in_memory_store[source_key]

        # Filter for the specific entry_id
        filtered_df = df.filter(pl.col("__entry_id") == entry_id)

        if filtered_df.shape[0] == 0:
            return None

        # Remove __entry_id column and convert to Arrow
        result_df = filtered_df.drop("__entry_id")
        return result_df.to_arrow()

    def get_all_records(
        self, source_name: str, source_id: str, add_entry_id_column: bool | str = False
    ) -> pa.Table | None:
        """Retrieve all records for a given source as a single table."""
        df = self.get_all_records_as_polars(
            source_name, source_id, add_entry_id_column=add_entry_id_column
        )
        if df is None:
            return None
        return df.collect().to_arrow()

    def get_all_records_as_polars(
        self, source_name: str, source_id: str, add_entry_id_column: bool | str = False
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a single Polars LazyFrame."""
        source_key = self._get_source_key(source_name, source_id)

        if source_key not in self._in_memory_store:
            return None

        df = self._in_memory_store[source_key]

        if df.shape[0] == 0:
            return None

        # perform column selection lazily
        df = df.lazy()

        # Handle entry_id column based on parameter
        if add_entry_id_column is False:
            # Remove __entry_id column
            result_df = df.drop("__entry_id")
        elif add_entry_id_column is True:
            # Keep __entry_id column as is
            result_df = df
        elif isinstance(add_entry_id_column, str):
            # Rename __entry_id to custom name
            result_df = df.rename({"__entry_id": add_entry_id_column})
        else:
            raise ValueError(
                f"add_entry_id_column must be a bool or str but {add_entry_id_column} was given"
            )

        return result_df

    def get_records_by_ids(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve records by entry IDs as a single table.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Arrow table containing all found records, or None if no records found
        """
        # Convert input to Polars Series
        if isinstance(entry_ids, list):
            if not entry_ids:
                return None
            entry_ids_series = pl.Series("entry_id", entry_ids)
        elif isinstance(entry_ids, pl.Series):
            if len(entry_ids) == 0:
                return None
            entry_ids_series = entry_ids
        elif isinstance(entry_ids, pa.Array):
            if len(entry_ids) == 0:
                return None
            entry_ids_series: pl.Series = pl.from_arrow(
                pa.table({"entry_id": entry_ids})
            )["entry_id"]  # type: ignore
        else:
            raise TypeError(
                f"entry_ids must be list[str], pl.Series, or pa.Array, got {type(entry_ids)}"
            )

        source_key = self._get_source_key(source_name, source_id)

        if source_key not in self._in_memory_store:
            return None

        df = self._in_memory_store[source_key]

        if preserve_input_order:
            # Create DataFrame with input order and join to preserve order with nulls
            ordered_df = pl.DataFrame({"__entry_id": entry_ids_series})
            result_df = ordered_df.join(df, on="__entry_id", how="left")
        else:
            # Filter for matching entry_ids (storage order)
            result_df = df.filter(pl.col("__entry_id").is_in(entry_ids_series))

        if result_df.shape[0] == 0:
            return None

        # Handle entry_id column based on parameter
        if add_entry_id_column is False:
            # Remove __entry_id column
            result_df = result_df.drop("__entry_id")
        elif add_entry_id_column is True:
            # Keep __entry_id column as is
            pass
        elif isinstance(add_entry_id_column, str):
            # Rename __entry_id to custom name
            result_df = result_df.rename({"__entry_id": add_entry_id_column})

        return result_df.to_arrow()

    def get_records_by_ids_as_polars(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        """
        Retrieve records by entry IDs as a single Polars LazyFrame.

        Args:
            source_name: Name of the data source
            source_id: ID of the specific dataset within the source
            entry_ids: Entry IDs to retrieve. Can be:
                - list[str]: List of entry ID strings
                - pl.Series: Polars Series containing entry IDs
                - pa.Array: PyArrow Array containing entry IDs
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name
            preserve_input_order: If True, return results in the same order as input entry_ids,
                with null rows for missing entries. If False, return in storage order.

        Returns:
            Polars LazyFrame containing all found records, or None if no records found
        """
        # Get Arrow result and convert to Polars LazyFrame
        arrow_result = self.get_records_by_ids(
            source_name, source_id, entry_ids, add_entry_id_column, preserve_input_order
        )

        if arrow_result is None:
            return None

        # Convert to Polars LazyFrame
        df = cast(pl.DataFrame, pl.from_arrow(arrow_result))
        return df.lazy()

    def entry_exists(self, source_name: str, source_id: str, entry_id: str) -> bool:
        """Check if a specific entry exists."""
        source_key = self._get_source_key(source_name, source_id)

        if source_key not in self._in_memory_store:
            return False

        df = self._in_memory_store[source_key]
        return df.filter(pl.col("__entry_id") == entry_id).shape[0] > 0

    def list_entries(self, source_name: str, source_id: str) -> set[str]:
        """List all entry IDs for a specific source."""
        source_key = self._get_source_key(source_name, source_id)

        if source_key not in self._in_memory_store:
            return set()

        df = self._in_memory_store[source_key]
        return set(df["__entry_id"].to_list())

    def list_sources(self) -> set[tuple[str, str]]:
        """List all (source_name, source_id) combinations."""
        sources = set()
        for source_key in self._in_memory_store.keys():
            if ":" in source_key:
                source_name, source_id = source_key.split(":", 1)
                sources.add((source_name, source_id))
        return sources

    def clear_source(self, source_name: str, source_id: str) -> None:
        """Clear all records for a specific source."""
        source_key = self._get_source_key(source_name, source_id)
        if source_key in self._in_memory_store:
            del self._in_memory_store[source_key]
            logger.debug(f"Cleared source {source_key}")

    def clear_all(self) -> None:
        """Clear all records from the store."""
        self._in_memory_store.clear()
        logger.info("Cleared all records from store")

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive statistics about the data store."""
        total_records = 0
        total_memory_mb = 0
        source_stats = []

        for source_key, df in self._in_memory_store.items():
            record_count = df.shape[0]
            total_records += record_count

            # Estimate memory usage (rough approximation)
            memory_bytes = df.estimated_size()
            memory_mb = memory_bytes / (1024 * 1024)
            total_memory_mb += memory_mb

            source_stats.append(
                {
                    "source_key": source_key,
                    "record_count": record_count,
                    "column_count": df.shape[1] - 1,  # Exclude __entry_id
                    "memory_mb": round(memory_mb, 2),
                    "columns": [col for col in df.columns if col != "__entry_id"],
                }
            )

        return {
            "total_records": total_records,
            "total_sources": len(self._in_memory_store),
            "total_memory_mb": round(total_memory_mb, 2),
            "duplicate_entry_behavior": self.duplicate_entry_behavior,
            "source_details": source_stats,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo_single_row_constraint()
