import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import polars as pl
import os
import json
import threading
import time
from pathlib import Path
from typing import Any, cast
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from collections import defaultdict


# Module-level logger
logger = logging.getLogger(__name__)


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
                    logger.debug(f"Reordering columns to match existing schema")
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
        system_columns = [
            ("__source_name", pa.array([metadata.source_name] * len(table))),
            ("__source_id", pa.array([metadata.source_id] * len(table))),
            ("__entry_id", pa.array([metadata.entry_id] * len(table))),
            ("__created_at", pa.array([metadata.created_at] * len(table))),
            ("__updated_at", pa.array([metadata.updated_at] * len(table))),
            ("__schema_hash", pa.array([metadata.schema_hash] * len(table))),
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
        system_cols = [
            "__source_name",
            "__source_id",
            "__entry_id",
            "__created_at",
            "__updated_at",
            "__schema_hash",
        ]
        user_columns = [name for name in table.column_names if name not in system_cols]
        return table.select(user_columns)

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
            raise ValueError(
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

    def get_all_records(self, source_name: str, source_id: str) -> pa.Table | None:
        """Retrieve all records for a given source as a single Arrow table."""
        cache = self._get_or_create_source_cache(source_name, source_id)
        table = cache.get_all_entries()

        if table is None:
            return None

        return self._remove_system_columns(table)

    def get_all_records_as_polars(
        self, source_name: str, source_id: str
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a Polars LazyFrame."""
        cache = self._get_or_create_source_cache(source_name, source_id)
        lazy_frame = cache.get_all_entries_as_polars()

        if lazy_frame is None:
            return None

        # Remove system columns
        system_cols = ["__entry_id", "__created_at", "__updated_at", "__schema_hash"]
        user_columns = [col for col in lazy_frame.columns if col not in system_cols]

        return lazy_frame.select(user_columns)

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
                result = store.add_record("experiments", "dataset_A", entry_id, data)
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
            print(f"✓ Overwrote existing record")

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
            print(f"\n=== Final Statistics ===")
            print(f"Total records: {stats['total_records']}")
            print(f"Loaded caches: {stats['loaded_source_caches']}")
            print(f"Dirty caches: {stats['dirty_caches']}")

        finally:
            store.shutdown()

    print("\n✓ Single-row constraint testing completed successfully!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo_single_row_constraint()
