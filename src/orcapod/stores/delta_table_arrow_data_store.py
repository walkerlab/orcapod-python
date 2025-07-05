import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import polars as pl
from pathlib import Path
from typing import Any
import logging
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from collections import defaultdict


# Module-level logger
logger = logging.getLogger(__name__)


class DeltaTableArrowDataStore:
    """
    Delta Table-based Arrow data store with flexible hierarchical path support and schema preservation.

    Uses tuple-based source paths for robust parameter handling:
    - ("source_name", "source_id") -> source_name/source_id/
    - ("org", "project", "dataset") -> org/project/dataset/
    - ("year", "month", "day", "experiment") -> year/month/day/experiment/
    """

    def __init__(
        self,
        base_path: str | Path,
        duplicate_entry_behavior: str = "error",
        create_base_path: bool = True,
        max_hierarchy_depth: int = 10,
        batch_size: int = 100,
    ):
        """
        Initialize the DeltaTableArrowDataStore.

        Args:
            base_path: Base directory path where Delta tables will be stored
            duplicate_entry_behavior: How to handle duplicate entry_ids:
                - 'error': Raise ValueError when entry_id already exists
                - 'overwrite': Replace existing entry with new data
            create_base_path: Whether to create the base path if it doesn't exist
            max_hierarchy_depth: Maximum allowed depth for source paths (safety limit)
            batch_size: Number of records to batch before writing to Delta table
            auto_flush_interval: Time in seconds to auto-flush pending batches (0 to disable)
        """
        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")

        self.duplicate_entry_behavior = duplicate_entry_behavior
        self.base_path = Path(base_path)
        self.max_hierarchy_depth = max_hierarchy_depth
        self.batch_size = batch_size

        if create_base_path:
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif not self.base_path.exists():
            raise ValueError(
                f"Base path {self.base_path} does not exist and create_base_path=False"
            )

        # Cache for Delta tables to avoid repeated initialization
        self._delta_table_cache: dict[str, DeltaTable] = {}

        # Batch management
        self._pending_batches: dict[str, dict[str, pa.Table]] = defaultdict(dict)

        logger.info(
            f"Initialized DeltaTableArrowDataStore at {self.base_path} "
            f"with duplicate_entry_behavior='{duplicate_entry_behavior}', "
            f"batch_size={batch_size}, as"
        )

    def flush(self) -> None:
        """
        Flush all pending batches immediately.

        This method is called to ensure all pending data is written to the Delta tables.
        """
        try:
            self.flush_all_batches()
        except Exception as e:
            logger.error(f"Error during flush: {e}")

    def flush_batch(self, source_path: tuple[str, ...]) -> None:
        """
        Flush pending batch for a specific source path.

        Args:
            source_path: Tuple of path components
        """
        logger.debug("Flushing triggered!!")
        source_key = self._get_source_key(source_path)

        if (
            source_key not in self._pending_batches
            or not self._pending_batches[source_key]
        ):
            return

        # Get all pending records
        pending_tables = self._pending_batches[source_key]
        self._pending_batches[source_key] = {}

        try:
            # Combine all tables in the batch
            combined_table = pa.concat_tables(pending_tables.values()).combine_chunks()

            table_path = self._get_table_path(source_path)
            table_path.mkdir(parents=True, exist_ok=True)

            # Check if table exists
            delta_table = self._get_existing_delta_table(source_path)

            if delta_table is None:
                # TODO: reconsider mode="overwrite" here
                write_deltalake(
                    table_path,
                    combined_table,
                    mode="overwrite",
                )
                logger.debug(
                    f"Created new Delta table for {source_key} with {len(combined_table)} records"
                )
            else:
                if self.duplicate_entry_behavior == "overwrite":
                    # Get entry IDs from the batch
                    entry_ids = combined_table.column("__entry_id").to_pylist()
                    unique_entry_ids = list(set(entry_ids))

                    # Delete existing records with these IDs
                    if unique_entry_ids:
                        entry_ids_str = "', '".join(unique_entry_ids)
                        delete_predicate = f"__entry_id IN ('{entry_ids_str}')"
                        try:
                            delta_table.delete(delete_predicate)
                            logger.debug(
                                f"Deleted {len(unique_entry_ids)} existing records from {source_key}"
                            )
                        except Exception as e:
                            logger.debug(
                                f"No existing records to delete from {source_key}: {e}"
                            )

                # otherwise, only insert if same entry_id does not exist yet
                delta_table.merge(
                    source=combined_table,
                    predicate="target.__entry_id = source.__entry_id",
                    source_alias="source",
                    target_alias="target",
                ).when_not_matched_insert_all().execute()

                logger.debug(
                    f"Appended batch of {len(combined_table)} records to {source_key}"
                )

            # Update cache
            self._delta_table_cache[source_key] = DeltaTable(str(table_path))

        except Exception as e:
            logger.error(f"Error flushing batch for {source_key}: {e}")
            # Put the tables back in the pending queue
            self._pending_batches[source_key] = pending_tables
            raise

    def flush_all_batches(self) -> None:
        """Flush all pending batches."""
        source_keys = list(self._pending_batches.keys())

        # TODO: capture and re-raise exceptions at the end
        for source_key in source_keys:
            source_path = tuple(source_key.split("/"))
            try:
                self.flush_batch(source_path)
            except Exception as e:
                logger.error(f"Error flushing batch for {source_key}: {e}")

    def __del__(self):
        """Cleanup when object is destroyed."""
        self.flush()

    def _validate_source_path(self, source_path: tuple[str, ...]) -> None:
        # TODO: consider removing this as path creation can be tried directly
        """
        Validate source path components.

        Args:
            source_path: Tuple of path components

        Raises:
            ValueError: If path is invalid
        """
        if not source_path:
            raise ValueError("Source path cannot be empty")

        if len(source_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"Source path depth {len(source_path)} exceeds maximum {self.max_hierarchy_depth}"
            )

        # Validate path components
        for i, component in enumerate(source_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"Source path component {i} is invalid: {repr(component)}"
                )

            # Check for filesystem-unsafe characters
            unsafe_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", "\0"]
            if any(char in component for char in unsafe_chars):
                raise ValueError(
                    f"Source path component contains invalid characters: {repr(component)}"
                )

    def _get_source_key(self, source_path: tuple[str, ...]) -> str:
        """Generate cache key for source storage."""
        return "/".join(source_path)

    def _get_table_path(self, source_path: tuple[str, ...]) -> Path:
        """Get the filesystem path for a given source path."""
        path = self.base_path
        for subpath in source_path:
            path = path / subpath
        return path

    def _get_existing_delta_table(
        self, source_path: tuple[str, ...]
    ) -> DeltaTable | None:
        """
        Get or create a Delta table, handling schema initialization properly.

        Args:
            source_path: Tuple of path components

        Returns:
            DeltaTable instance or None if table doesn't exist
        """
        source_key = self._get_source_key(source_path)
        table_path = self._get_table_path(source_path)

        # Check cache first
        if dt := self._delta_table_cache.get(source_key):
            return dt

        try:
            # Try to load existing table
            delta_table = DeltaTable(str(table_path))
            self._delta_table_cache[source_key] = delta_table
            logger.debug(f"Loaded existing Delta table for {source_key}")
            return delta_table
        except TableNotFoundError:
            # Table doesn't exist
            return None
        except Exception as e:
            logger.error(f"Error loading Delta table for {source_key}: {e}")
            # Try to clear any corrupted cache and retry once
            if source_key in self._delta_table_cache:
                del self._delta_table_cache[source_key]
            return None

    def _ensure_entry_id_column(self, arrow_data: pa.Table, entry_id: str) -> pa.Table:
        """Ensure the table has an __entry_id column."""
        if "__entry_id" not in arrow_data.column_names:
            # Add entry_id column at the beginning
            key_array = pa.array([entry_id] * len(arrow_data), type=pa.large_string())
            arrow_data = arrow_data.add_column(0, "__entry_id", key_array)
        return arrow_data

    def _remove_entry_id_column(self, arrow_data: pa.Table) -> pa.Table:
        """Remove the __entry_id column if it exists."""
        if "__entry_id" in arrow_data.column_names:
            column_names = arrow_data.column_names
            indices_to_keep = [
                i for i, name in enumerate(column_names) if name != "__entry_id"
            ]
            arrow_data = arrow_data.select(indices_to_keep)
        return arrow_data

    def _handle_entry_id_column(
        self, arrow_data: pa.Table, add_entry_id_column: bool | str = False
    ) -> pa.Table:
        """
        Handle entry_id column based on add_entry_id_column parameter.

        Args:
            arrow_data: Arrow table with __entry_id column
            add_entry_id_column: Control entry ID column inclusion:
                - False: Remove __entry_id column
                - True: Keep __entry_id column as is
                - str: Rename __entry_id column to custom name
        """
        if add_entry_id_column is False:
            # Remove the __entry_id column
            return self._remove_entry_id_column(arrow_data)
        elif isinstance(add_entry_id_column, str):
            # Rename __entry_id to custom name
            if "__entry_id" in arrow_data.column_names:
                schema = arrow_data.schema
                new_names = [
                    add_entry_id_column if name == "__entry_id" else name
                    for name in schema.names
                ]
                return arrow_data.rename_columns(new_names)
        # If add_entry_id_column is True, keep __entry_id as is
        return arrow_data

    def _create_entry_id_filter(self, entry_id: str) -> list:
        """
        Create a proper filter expression for Delta Lake.

        Args:
            entry_id: The entry ID to filter by

        Returns:
            List containing the filter expression for Delta Lake
        """
        return [("__entry_id", "=", entry_id)]

    def _create_entry_ids_filter(self, entry_ids: list[str]) -> list:
        """
        Create a proper filter expression for multiple entry IDs.

        Args:
            entry_ids: List of entry IDs to filter by

        Returns:
            List containing the filter expression for Delta Lake
        """
        return [("__entry_id", "in", entry_ids)]

    def _read_table_with_filter(
        self,
        delta_table: DeltaTable,
        filters: list | None = None,
    ) -> pa.Table:
        """
        Read table using to_pyarrow_dataset with original schema preservation.

        Args:
            delta_table: The Delta table to read from
            filters: Optional filters to apply

        Returns:
            Arrow table with preserved schema
        """
        # Use to_pyarrow_dataset with as_large_types for Polars compatible arrow table loading
        dataset: ds.Dataset = delta_table.to_pyarrow_dataset(as_large_types=True)
        if filters:
            # Apply filters at dataset level for better performance
            import pyarrow.compute as pc

            filter_expr = None
            for filt in filters:
                if len(filt) == 3:
                    col, op, val = filt
                    if op == "=":
                        expr = pc.equal(pc.field(col), pa.scalar(val))  # type: ignore
                    elif op == "in":
                        expr = pc.is_in(pc.field(col), pa.array(val))  # type: ignore
                    else:
                        logger.warning(
                            f"Unsupported filter operation: {op}. Falling back to table-level filter application which may be less efficient."
                        )
                        # Fallback to table-level filtering
                        return dataset.to_table()(filters=filters)

                    if filter_expr is None:
                        filter_expr = expr
                    else:
                        filter_expr = pc.and_(filter_expr, expr)  # type: ignore

            if filter_expr is not None:
                return dataset.to_table(filter=filter_expr)

        return dataset.to_table()

    def add_record(
        self,
        source_path: tuple[str, ...],
        entry_id: str,
        arrow_data: pa.Table,
        force_flush: bool = False,
    ) -> pa.Table:
        """
        Add a record to the Delta table (batched).

        Args:
            source_path: Tuple of path components (e.g., ("org", "project", "dataset"))
            entry_id: Unique identifier for this record
            arrow_data: The Arrow table data to store
            ignore_duplicate: If True, ignore duplicate entry error
            force_flush: If True, immediately flush this record to disk

        Returns:
            The Arrow table data that was stored

        Raises:
            ValueError: If entry_id already exists and duplicate_entry_behavior is 'error'
        """
        self._validate_source_path(source_path)
        source_key = self._get_source_key(source_path)

        # Check for existing entry
        if self.duplicate_entry_behavior == "error":
            # Only check existing table, not pending batch for performance
            pending_table = self._pending_batches[source_key].get(entry_id, None)
            if pending_table is not None:
                raise ValueError(
                    f"Entry '{entry_id}' already exists in pending batch for {source_key}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )
            existing_record = self.get_record(source_path, entry_id, flush=False)
            if existing_record is not None:
                raise ValueError(
                    f"Entry '{entry_id}' already exists in {'/'.join(source_path)}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )

        # Add entry_id column to the data
        data_with_entry_id = self._ensure_entry_id_column(arrow_data, entry_id)

        if force_flush:
            # Write immediately
            table_path = self._get_table_path(source_path)
            table_path.mkdir(parents=True, exist_ok=True)

            delta_table = self._get_existing_delta_table(source_path)

            if delta_table is None:
                # Create new table - save original schema first
                write_deltalake(str(table_path), data_with_entry_id, mode="overwrite")
                logger.debug(f"Created new Delta table for {source_key}")
            else:
                if self.duplicate_entry_behavior == "overwrite":
                    try:
                        delta_table.delete(
                            f"__entry_id = '{entry_id.replace(chr(39), chr(39) + chr(39))}'"
                        )
                        logger.debug(
                            f"Deleted existing record {entry_id} from {source_key}"
                        )
                    except Exception as e:
                        logger.debug(
                            f"No existing record to delete for {entry_id}: {e}"
                        )

                write_deltalake(
                    table_path,
                    data_with_entry_id,
                    mode="append",
                    schema_mode="merge",
                )

            # Update cache
            self._delta_table_cache[source_key] = DeltaTable(str(table_path))
        else:
            # Add to the batch for later flushing
            self._pending_batches[source_key][entry_id] = data_with_entry_id
            batch_size = len(self._pending_batches[source_key])

            # Check if we need to flush
            if batch_size >= self.batch_size:
                self.flush_batch(source_path)

        logger.debug(f"Added record {entry_id} to {source_key}")
        return arrow_data

    def get_pending_batch_info(self) -> dict[str, int]:
        """
        Get information about pending batches.

        Returns:
            Dictionary mapping source keys to number of pending records
        """
        return {
            source_key: len(tables)
            for source_key, tables in self._pending_batches.items()
            if tables
        }

    def get_record(
        self, source_path: tuple[str, ...], entry_id: str, flush: bool = False
    ) -> pa.Table | None:
        """
        Get a specific record by entry_id with schema preservation.

        Args:
            source_path: Tuple of path components
            entry_id: Unique identifier for the record

        Returns:
            Arrow table for the record or None if not found
        """
        if flush:
            self.flush_batch(source_path)
        self._validate_source_path(source_path)

        # check if entry_id is found in pending batches
        source_key = self._get_source_key(source_path)
        if entry_id in self._pending_batches[source_key]:
            # Return the pending record directly
            return self._pending_batches[source_key][entry_id]

        delta_table = self._get_existing_delta_table(source_path)
        if delta_table is None:
            return None

        try:
            # Use schema-preserving read
            filter_expr = self._create_entry_id_filter(entry_id)
            result = self._read_table_with_filter(delta_table, filters=filter_expr)

            if len(result) == 0:
                return None

            # Remove the __entry_id column before returning
            return self._remove_entry_id_column(result)

        except Exception as e:
            logger.error(
                f"Error getting record {entry_id} from {'/'.join(source_path)}: {e}"
            )
            raise e

    def get_all_records(
        self,
        source_path: tuple[str, ...],
        add_entry_id_column: bool | str = False,
        retrieve_pending: bool = True,
        flush: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve all records for a given source path as a single table with schema preservation.

        Args:
            source_path: Tuple of path components
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name

        Returns:
            Arrow table containing all records with original schema, or None if no records found
        """
        if flush:
            self.flush_batch(source_path)
        self._validate_source_path(source_path)

        collected_arrays = []
        if retrieve_pending:
            # Check if there are pending records in the batch
            for entry_id, arrow_table in self._pending_batches[
                self._get_source_key(source_path)
            ].items():
                collected_arrays.append(
                    self._ensure_entry_id_column(arrow_table, entry_id)
                )

        delta_table = self._get_existing_delta_table(source_path)
        if delta_table is not None:
            try:
                # Use filter-based read
                result = self._read_table_with_filter(delta_table)

                if len(result) != 0:
                    collected_arrays.append(result)

            except Exception as e:
                logger.error(
                    f"Error getting all records from {'/'.join(source_path)}: {e}"
                )
        if collected_arrays:
            total_table = pa.Table.concatenate(collected_arrays)

            # Handle entry_id column based on parameter
            return self._handle_entry_id_column(total_table, add_entry_id_column)

        return None

    def get_all_records_as_polars(
        self, source_path: tuple[str, ...], flush: bool = True
    ) -> pl.LazyFrame | None:
        """
        Retrieve all records for a given source path as a single Polars LazyFrame.

        Args:
            source_path: Tuple of path components

        Returns:
            Polars LazyFrame containing all records, or None if no records found
        """
        all_records = self.get_all_records(source_path, flush=flush)
        if all_records is None:
            return None
        return pl.LazyFrame(all_records)

    def get_records_by_ids(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
        flush: bool = False,
    ) -> pa.Table | None:
        """
        Retrieve records by entry IDs as a single table with schema preservation.

        Args:
            source_path: Tuple of path components
            entry_ids: Entry IDs to retrieve
            add_entry_id_column: Control entry ID column inclusion
            preserve_input_order: If True, return results in input order with nulls for missing

        Returns:
            Arrow table containing all found records with original schema, or None if no records found
        """
        if flush:
            self.flush_batch(source_path)

        self._validate_source_path(source_path)

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

        delta_table = self._get_existing_delta_table(source_path)
        if delta_table is None:
            return None

        try:
            # Use schema-preserving read with filters
            filter_expr = self._create_entry_ids_filter(entry_ids_list)
            result = self._read_table_with_filter(delta_table, filters=filter_expr)

            if len(result) == 0:
                return None

            if preserve_input_order:
                # Need to reorder results and add nulls for missing entries
                import pandas as pd

                df = result.to_pandas()
                df = df.set_index("__entry_id")

                # Create a DataFrame with the desired order, filling missing with NaN
                ordered_df = df.reindex(entry_ids_list)

                # Convert back to Arrow
                result = pa.Table.from_pandas(ordered_df.reset_index())

            # Handle entry_id column based on parameter
            return self._handle_entry_id_column(result, add_entry_id_column)

        except Exception as e:
            logger.error(
                f"Error getting records by IDs from {'/'.join(source_path)}: {e}"
            )
            return None

    def get_records_by_ids_as_polars(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
        flush: bool = False,
    ) -> pl.LazyFrame | None:
        """
        Retrieve records by entry IDs as a single Polars LazyFrame.

        Args:
            source_path: Tuple of path components
            entry_ids: Entry IDs to retrieve
            add_entry_id_column: Control entry ID column inclusion
            preserve_input_order: If True, return results in input order with nulls for missing

        Returns:
            Polars LazyFrame containing all found records, or None if no records found
        """
        arrow_result = self.get_records_by_ids(
            source_path,
            entry_ids,
            add_entry_id_column,
            preserve_input_order,
            flush=flush,
        )

        if arrow_result is None:
            return None

        # Convert to Polars LazyFrame
        return pl.LazyFrame(arrow_result)

    # Additional utility methods
    def list_sources(self) -> list[tuple[str, ...]]:
        """
        List all available source paths.

        Returns:
            List of source path tuples
        """
        sources = []

        def _scan_directory(current_path: Path, path_components: tuple[str, ...]):
            """Recursively scan for Delta tables."""
            for item in current_path.iterdir():
                if not item.is_dir():
                    continue

                new_path_components = path_components + (item.name,)

                # Check if this directory contains a Delta table
                try:
                    DeltaTable(str(item))
                    sources.append(new_path_components)
                except TableNotFoundError:
                    # Not a Delta table, continue scanning subdirectories
                    if len(new_path_components) < self.max_hierarchy_depth:
                        _scan_directory(item, new_path_components)

        _scan_directory(self.base_path, ())
        return sources

    def delete_source(self, source_path: tuple[str, ...]) -> bool:
        """
        Delete an entire source (all records for a source path).

        Args:
            source_path: Tuple of path components

        Returns:
            True if source was deleted, False if it didn't exist
        """
        self._validate_source_path(source_path)

        # Flush any pending batches first
        self.flush_batch(source_path)

        table_path = self._get_table_path(source_path)
        source_key = self._get_source_key(source_path)

        if not table_path.exists():
            return False

        try:
            # Remove from caches
            if source_key in self._delta_table_cache:
                del self._delta_table_cache[source_key]
            if source_key in self._schema_cache:
                del self._schema_cache[source_key]

            # Remove directory
            import shutil

            shutil.rmtree(table_path)

            logger.info(f"Deleted source {source_key}")
            return True

        except Exception as e:
            logger.error(f"Error deleting source {source_key}: {e}")
            return False

    def delete_record(self, source_path: tuple[str, ...], entry_id: str) -> bool:
        """
        Delete a specific record.

        Args:
            source_path: Tuple of path components
            entry_id: ID of the record to delete

        Returns:
            True if record was deleted, False if it didn't exist
        """
        self._validate_source_path(source_path)

        # Flush any pending batches first
        self._flush_batch(source_path)

        delta_table = self._get_existing_delta_table(source_path)
        if delta_table is None:
            return False

        try:
            # Check if record exists using proper filter
            filter_expr = self._create_entry_id_filter(entry_id)
            existing = self._read_table_with_filter(delta_table, filters=filter_expr)
            if len(existing) == 0:
                return False

            # Delete the record using SQL-style predicate (this is correct for delete operations)
            delta_table.delete(
                f"__entry_id = '{entry_id.replace(chr(39), chr(39) + chr(39))}'"
            )

            # Update cache
            source_key = self._get_source_key(source_path)
            self._delta_table_cache[source_key] = delta_table

            logger.debug(f"Deleted record {entry_id} from {'/'.join(source_path)}")
            return True

        except Exception as e:
            logger.error(
                f"Error deleting record {entry_id} from {'/'.join(source_path)}: {e}"
            )
            return False

    def get_table_info(self, source_path: tuple[str, ...]) -> dict[str, Any] | None:
        """
        Get metadata information about a Delta table.

        Args:
            source_path: Tuple of path components

        Returns:
            Dictionary with table metadata, or None if table doesn't exist
        """
        self._validate_source_path(source_path)

        delta_table = self._get_existing_delta_table(source_path)
        if delta_table is None:
            return None

        try:
            # Get basic info
            schema = delta_table.schema()
            history = delta_table.history()
            source_key = self._get_source_key(source_path)

            # Add pending batch info
            pending_info = self.get_pending_batch_info()
            pending_count = pending_info.get(source_key, 0)

            return {
                "path": str(self._get_table_path(source_path)),
                "source_path": source_path,
                "schema": schema,
                "version": delta_table.version(),
                "num_files": len(delta_table.files()),
                "history_length": len(history),
                "latest_commit": history[0] if history else None,
                "pending_records": pending_count,
            }

        except Exception as e:
            logger.error(f"Error getting table info for {'/'.join(source_path)}: {e}")
            return None
