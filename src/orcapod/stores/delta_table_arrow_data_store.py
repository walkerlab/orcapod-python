import pyarrow as pa
import polars as pl
from pathlib import Path
from typing import Any, Union
import logging
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

# Module-level logger
logger = logging.getLogger(__name__)


class DeltaTableArrowDataStore:
    """
    Delta Table-based Arrow data store with flexible hierarchical path support.

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
        """
        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")

        self.duplicate_entry_behavior = duplicate_entry_behavior
        self.base_path = Path(base_path)
        self.max_hierarchy_depth = max_hierarchy_depth

        if create_base_path:
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif not self.base_path.exists():
            raise ValueError(
                f"Base path {self.base_path} does not exist and create_base_path=False"
            )

        # Cache for Delta tables to avoid repeated initialization
        self._delta_table_cache: dict[str, DeltaTable] = {}

        logger.info(
            f"Initialized DeltaTableArrowDataStore at {self.base_path} "
            f"with duplicate_entry_behavior='{duplicate_entry_behavior}'"
        )

    def _validate_source_path(self, source_path: tuple[str, ...]) -> None:
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
        for component in source_path:
            path = path / component
        return path

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

    def add_record(
        self,
        source_path: tuple[str, ...],
        entry_id: str,
        arrow_data: pa.Table,
        ignore_duplicate: bool = False,
    ) -> pa.Table:
        """
        Add a record to the Delta table.

        Args:
            source_path: Tuple of path components (e.g., ("org", "project", "dataset"))
            entry_id: Unique identifier for this record
            arrow_data: The Arrow table data to store
            ignore_duplicate: If True, ignore duplicate entry error

        Returns:
            The Arrow table data that was stored

        Raises:
            ValueError: If entry_id already exists and duplicate_entry_behavior is 'error'
        """
        self._validate_source_path(source_path)

        table_path = self._get_table_path(source_path)
        source_key = self._get_source_key(source_path)

        # Ensure directory exists
        table_path.mkdir(parents=True, exist_ok=True)

        # Add entry_id column to the data
        data_with_entry_id = self._ensure_entry_id_column(arrow_data, entry_id)

        # Check for existing entry if needed
        if not ignore_duplicate and self.duplicate_entry_behavior == "error":
            existing_record = self.get_record(source_path, entry_id)
            if existing_record is not None:
                raise ValueError(
                    f"Entry '{entry_id}' already exists in {'/'.join(source_path)}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )

        try:
            # Try to load existing table
            delta_table = DeltaTable(str(table_path))

            if self.duplicate_entry_behavior == "overwrite":
                # Delete existing record if it exists, then append new one
                try:
                    # First, delete existing record with this entry_id
                    delta_table.delete(f"__entry_id = '{entry_id}'")
                    logger.debug(
                        f"Deleted existing record {entry_id} from {source_key}"
                    )
                except Exception as e:
                    # If delete fails (e.g., record doesn't exist), that's fine
                    logger.debug(f"No existing record to delete for {entry_id}: {e}")

            # Append new record
            write_deltalake(
                str(table_path), data_with_entry_id, mode="append", schema_mode="merge"
            )

        except TableNotFoundError:
            # Table doesn't exist, create it
            write_deltalake(str(table_path), data_with_entry_id, mode="overwrite")
            logger.debug(f"Created new Delta table for {source_key}")

        # Update cache
        self._delta_table_cache[source_key] = DeltaTable(str(table_path))

        logger.debug(f"Added record {entry_id} to {source_key}")
        return arrow_data

    def get_record(
        self, source_path: tuple[str, ...], entry_id: str
    ) -> pa.Table | None:
        """
        Get a specific record by entry_id.

        Args:
            source_path: Tuple of path components
            entry_id: Unique identifier for the record

        Returns:
            Arrow table for the record, or None if not found
        """
        self._validate_source_path(source_path)

        table_path = self._get_table_path(source_path)

        try:
            delta_table = DeltaTable(str(table_path))

            # Query for the specific entry_id
            result = delta_table.to_pyarrow_table(filter=f"__entry_id = '{entry_id}'")

            if len(result) == 0:
                return None

            # Remove the __entry_id column before returning
            return self._remove_entry_id_column(result)

        except TableNotFoundError:
            return None
        except Exception as e:
            logger.error(
                f"Error getting record {entry_id} from {'/'.join(source_path)}: {e}"
            )
            return None

    def get_all_records(
        self, source_path: tuple[str, ...], add_entry_id_column: bool | str = False
    ) -> pa.Table | None:
        """
        Retrieve all records for a given source path as a single table.

        Args:
            source_path: Tuple of path components
            add_entry_id_column: Control entry ID column inclusion:
                - False: Don't include entry ID column (default)
                - True: Include entry ID column as "__entry_id"
                - str: Include entry ID column with custom name

        Returns:
            Arrow table containing all records, or None if no records found
        """
        self._validate_source_path(source_path)

        table_path = self._get_table_path(source_path)

        try:
            delta_table = DeltaTable(str(table_path))
            result = delta_table.to_pyarrow_table()

            if len(result) == 0:
                return None

            # Handle entry_id column based on parameter
            return self._handle_entry_id_column(result, add_entry_id_column)

        except TableNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Error getting all records from {'/'.join(source_path)}: {e}")
            return None

    def get_all_records_as_polars(
        self, source_path: tuple[str, ...]
    ) -> pl.LazyFrame | None:
        """
        Retrieve all records for a given source path as a single Polars LazyFrame.

        Args:
            source_path: Tuple of path components

        Returns:
            Polars LazyFrame containing all records, or None if no records found
        """
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
            source_path: Tuple of path components
            entry_ids: Entry IDs to retrieve
            add_entry_id_column: Control entry ID column inclusion
            preserve_input_order: If True, return results in input order with nulls for missing

        Returns:
            Arrow table containing all found records, or None if no records found
        """
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

        table_path = self._get_table_path(source_path)

        try:
            delta_table = DeltaTable(str(table_path))

            # Create filter for the entry IDs - escape single quotes in IDs
            escaped_ids = [id_.replace("'", "''") for id_ in entry_ids_list]
            id_filter = " OR ".join([f"__entry_id = '{id_}'" for id_ in escaped_ids])

            result = delta_table.to_pyarrow_table(filter=id_filter)

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

        except TableNotFoundError:
            return None
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
            source_path, entry_ids, add_entry_id_column, preserve_input_order
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

        table_path = self._get_table_path(source_path)
        source_key = self._get_source_key(source_path)

        if not table_path.exists():
            return False

        try:
            # Remove from cache
            if source_key in self._delta_table_cache:
                del self._delta_table_cache[source_key]

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

        table_path = self._get_table_path(source_path)

        try:
            delta_table = DeltaTable(str(table_path))

            # Check if record exists
            escaped_entry_id = entry_id.replace("'", "''")
            existing = delta_table.to_pyarrow_table(
                filter=f"__entry_id = '{escaped_entry_id}'"
            )
            if len(existing) == 0:
                return False

            # Delete the record
            delta_table.delete(f"__entry_id = '{escaped_entry_id}'")

            # Update cache
            source_key = self._get_source_key(source_path)
            self._delta_table_cache[source_key] = delta_table

            logger.debug(f"Deleted record {entry_id} from {'/'.join(source_path)}")
            return True

        except TableNotFoundError:
            return False
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

        table_path = self._get_table_path(source_path)

        try:
            delta_table = DeltaTable(str(table_path))

            # Get basic info
            schema = delta_table.schema()
            history = delta_table.history()

            return {
                "path": str(table_path),
                "source_path": source_path,
                "schema": schema,
                "version": delta_table.version(),
                "num_files": len(delta_table.files()),
                "history_length": len(history),
                "latest_commit": history[0] if history else None,
            }

        except TableNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Error getting table info for {'/'.join(source_path)}: {e}")
            return None
