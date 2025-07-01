import polars as pl
import pyarrow as pa
import logging
from typing import Any, Dict, List, Tuple, cast
from collections import defaultdict

# Module-level logger
logger = logging.getLogger(__name__)


class ArrowBatchedPolarsDataStore:
    """
    Arrow-batched Polars data store that minimizes Arrow<->Polars conversions.

    Key optimizations:
    1. Keep data in Arrow format during batching
    2. Only convert to Polars when consolidating or querying
    3. Batch Arrow tables and concatenate before conversion
    4. Maintain Arrow-based indexing for fast lookups
    5. Lazy Polars conversion only when needed
    """

    def __init__(self, duplicate_entry_behavior: str = "error", batch_size: int = 100):
        """
        Initialize the ArrowBatchedPolarsDataStore.

        Args:
            duplicate_entry_behavior: How to handle duplicate entry_ids:
                - 'error': Raise ValueError when entry_id already exists
                - 'overwrite': Replace existing entry with new data
            batch_size: Number of records to batch before consolidating
        """
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")

        self.duplicate_entry_behavior = duplicate_entry_behavior
        self.batch_size = batch_size

        # Arrow batch buffer: {source_key: [(entry_id, arrow_table), ...]}
        self._arrow_batches: Dict[str, List[Tuple[str, pa.Table]]] = defaultdict(list)

        # Consolidated Polars store: {source_key: polars_dataframe}
        self._polars_store: Dict[str, pl.DataFrame] = {}

        # Entry ID index for fast lookups: {source_key: set[entry_ids]}
        self._entry_index: Dict[str, set] = defaultdict(set)

        # Schema cache
        self._schema_cache: Dict[str, pa.Schema] = {}

        logger.info(
            f"Initialized ArrowBatchedPolarsDataStore with "
            f"duplicate_entry_behavior='{duplicate_entry_behavior}', batch_size={batch_size}"
        )

    def _get_source_key(self, source_name: str, source_id: str) -> str:
        """Generate key for source storage."""
        return f"{source_name}:{source_id}"

    def _add_entry_id_to_arrow_table(self, table: pa.Table, entry_id: str) -> pa.Table:
        """Add entry_id column to Arrow table efficiently."""
        # Create entry_id array with the same length as the table
        entry_id_array = pa.array([entry_id] * len(table), type=pa.string())

        # Add column at the beginning for consistent ordering
        return table.add_column(0, "__entry_id", entry_id_array)

    def _consolidate_arrow_batch(self, source_key: str) -> None:
        """Consolidate Arrow batch into Polars DataFrame."""
        if source_key not in self._arrow_batches or not self._arrow_batches[source_key]:
            return

        logger.debug(
            f"Consolidating {len(self._arrow_batches[source_key])} Arrow tables for {source_key}"
        )

        # Prepare all Arrow tables with entry_id columns
        arrow_tables_with_id = []

        for entry_id, arrow_table in self._arrow_batches[source_key]:
            table_with_id = self._add_entry_id_to_arrow_table(arrow_table, entry_id)
            arrow_tables_with_id.append(table_with_id)

        # Concatenate all Arrow tables at once (very fast)
        if len(arrow_tables_with_id) == 1:
            consolidated_arrow = arrow_tables_with_id[0]
        else:
            consolidated_arrow = pa.concat_tables(arrow_tables_with_id)

        # Single conversion to Polars
        new_polars_df = cast(pl.DataFrame, pl.from_arrow(consolidated_arrow))

        # Combine with existing Polars DataFrame if it exists
        if source_key in self._polars_store:
            existing_df = self._polars_store[source_key]
            self._polars_store[source_key] = pl.concat([existing_df, new_polars_df])
        else:
            self._polars_store[source_key] = new_polars_df

        # Clear the Arrow batch
        self._arrow_batches[source_key].clear()

        logger.debug(
            f"Consolidated to Polars DataFrame with {len(self._polars_store[source_key])} total rows"
        )

    def _force_consolidation(self, source_key: str) -> None:
        """Force consolidation of Arrow batches."""
        if source_key in self._arrow_batches and self._arrow_batches[source_key]:
            self._consolidate_arrow_batch(source_key)

    def _get_consolidated_dataframe(self, source_key: str) -> pl.DataFrame | None:
        """Get consolidated Polars DataFrame, forcing consolidation if needed."""
        self._force_consolidation(source_key)
        return self._polars_store.get(source_key)

    def add_record(
        self,
        source_name: str,
        source_id: str,
        entry_id: str,
        arrow_data: pa.Table,
    ) -> pa.Table:
        """
        Add a record to the store using Arrow batching.

        This is the fastest path - no conversions, just Arrow table storage.
        """
        source_key = self._get_source_key(source_name, source_id)

        # Check for duplicate entry
        if entry_id in self._entry_index[source_key]:
            if self.duplicate_entry_behavior == "error":
                raise ValueError(
                    f"Entry '{entry_id}' already exists in {source_name}/{source_id}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )
            else:
                # Handle overwrite: remove from both Arrow batch and Polars store
                # Remove from Arrow batch
                self._arrow_batches[source_key] = [
                    (eid, table)
                    for eid, table in self._arrow_batches[source_key]
                    if eid != entry_id
                ]

                # Remove from Polars store if it exists
                if source_key in self._polars_store:
                    self._polars_store[source_key] = self._polars_store[
                        source_key
                    ].filter(pl.col("__entry_id") != entry_id)

        # Schema validation (cached)
        if source_key in self._schema_cache:
            if not self._schema_cache[source_key].equals(arrow_data.schema):
                raise ValueError(
                    f"Schema mismatch for {source_key}. "
                    f"Expected: {self._schema_cache[source_key]}, "
                    f"Got: {arrow_data.schema}"
                )
        else:
            self._schema_cache[source_key] = arrow_data.schema

        # Add to Arrow batch (no conversion yet!)
        self._arrow_batches[source_key].append((entry_id, arrow_data))
        self._entry_index[source_key].add(entry_id)

        # Consolidate if batch is full
        if len(self._arrow_batches[source_key]) >= self.batch_size:
            self._consolidate_arrow_batch(source_key)

        logger.debug(f"Added entry {entry_id} to Arrow batch for {source_key}")
        return arrow_data

    def get_record(
        self, source_name: str, source_id: str, entry_id: str
    ) -> pa.Table | None:
        """Get a specific record with optimized lookup."""
        source_key = self._get_source_key(source_name, source_id)

        # Quick existence check
        if entry_id not in self._entry_index[source_key]:
            return None

        # Check Arrow batch first (most recent data)
        for batch_entry_id, arrow_table in self._arrow_batches[source_key]:
            if batch_entry_id == entry_id:
                return arrow_table

        # Check consolidated Polars store
        df = self._get_consolidated_dataframe(source_key)
        if df is None:
            return None

        # Filter and convert back to Arrow
        filtered_df = df.filter(pl.col("__entry_id") == entry_id).drop("__entry_id")

        if filtered_df.height == 0:
            return None

        return filtered_df.to_arrow()

    def get_all_records(
        self, source_name: str, source_id: str, add_entry_id_column: bool | str = False
    ) -> pa.Table | None:
        """Retrieve all records as a single Arrow table."""
        source_key = self._get_source_key(source_name, source_id)

        # Force consolidation to include all data
        df = self._get_consolidated_dataframe(source_key)
        if df is None or df.height == 0:
            return None

        # Handle entry_id column
        if add_entry_id_column is False:
            result_df = df.drop("__entry_id")
        elif add_entry_id_column is True:
            result_df = df
        elif isinstance(add_entry_id_column, str):
            result_df = df.rename({"__entry_id": add_entry_id_column})
        else:
            result_df = df.drop("__entry_id")

        return result_df.to_arrow()

    def get_all_records_as_polars(
        self, source_name: str, source_id: str
    ) -> pl.LazyFrame | None:
        """Retrieve all records as a Polars LazyFrame."""
        source_key = self._get_source_key(source_name, source_id)

        df = self._get_consolidated_dataframe(source_key)
        if df is None or df.height == 0:
            return None

        return df.drop("__entry_id").lazy()

    def get_records_by_ids(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """Retrieve records by entry IDs efficiently."""
        # Convert input to list for processing
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
            raise TypeError(f"entry_ids must be list[str], pl.Series, or pa.Array")

        source_key = self._get_source_key(source_name, source_id)

        # Quick filter using index
        existing_entries = [
            entry_id
            for entry_id in entry_ids_list
            if entry_id in self._entry_index[source_key]
        ]

        if not existing_entries and not preserve_input_order:
            return None

        # Collect from Arrow batch first
        batch_tables = []
        found_in_batch = set()

        for entry_id, arrow_table in self._arrow_batches[source_key]:
            if entry_id in entry_ids_list:
                table_with_id = self._add_entry_id_to_arrow_table(arrow_table, entry_id)
                batch_tables.append(table_with_id)
                found_in_batch.add(entry_id)

        # Get remaining from consolidated store
        remaining_ids = [eid for eid in existing_entries if eid not in found_in_batch]

        consolidated_tables = []
        if remaining_ids:
            df = self._get_consolidated_dataframe(source_key)
            if df is not None:
                if preserve_input_order:
                    ordered_df = pl.DataFrame({"__entry_id": entry_ids_list})
                    result_df = ordered_df.join(df, on="__entry_id", how="left")
                else:
                    result_df = df.filter(pl.col("__entry_id").is_in(remaining_ids))

                if result_df.height > 0:
                    consolidated_tables.append(result_df.to_arrow())

        # Combine all results
        all_tables = batch_tables + consolidated_tables

        if not all_tables:
            return None

        # Concatenate Arrow tables
        if len(all_tables) == 1:
            result_table = all_tables[0]
        else:
            result_table = pa.concat_tables(all_tables)

        # Handle entry_id column
        if add_entry_id_column is False:
            # Remove __entry_id column
            column_names = result_table.column_names
            if "__entry_id" in column_names:
                indices = [
                    i for i, name in enumerate(column_names) if name != "__entry_id"
                ]
                result_table = result_table.select(indices)
        elif isinstance(add_entry_id_column, str):
            # Rename __entry_id column
            schema = result_table.schema
            new_names = [
                add_entry_id_column if name == "__entry_id" else name
                for name in schema.names
            ]
            result_table = result_table.rename_columns(new_names)

        return result_table

    def get_records_by_ids_as_polars(
        self,
        source_name: str,
        source_id: str,
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        """Retrieve records by entry IDs as Polars LazyFrame."""
        arrow_result = self.get_records_by_ids(
            source_name, source_id, entry_ids, add_entry_id_column, preserve_input_order
        )

        if arrow_result is None:
            return None

        pl_result = cast(pl.DataFrame, pl.from_arrow(arrow_result))

        return pl_result.lazy()

    def entry_exists(self, source_name: str, source_id: str, entry_id: str) -> bool:
        """Check if entry exists using the index."""
        source_key = self._get_source_key(source_name, source_id)
        return entry_id in self._entry_index[source_key]

    def list_entries(self, source_name: str, source_id: str) -> set[str]:
        """List all entry IDs using the index."""
        source_key = self._get_source_key(source_name, source_id)
        return self._entry_index[source_key].copy()

    def list_sources(self) -> set[tuple[str, str]]:
        """List all source combinations."""
        sources = set()
        for source_key in self._entry_index.keys():
            if ":" in source_key:
                source_name, source_id = source_key.split(":", 1)
                sources.add((source_name, source_id))
        return sources

    def force_consolidation(self) -> None:
        """Force consolidation of all Arrow batches."""
        for source_key in list(self._arrow_batches.keys()):
            self._force_consolidation(source_key)
        logger.info("Forced consolidation of all Arrow batches")

    def clear_source(self, source_name: str, source_id: str) -> None:
        """Clear all data for a source."""
        source_key = self._get_source_key(source_name, source_id)

        if source_key in self._arrow_batches:
            del self._arrow_batches[source_key]
        if source_key in self._polars_store:
            del self._polars_store[source_key]
        if source_key in self._entry_index:
            del self._entry_index[source_key]
        if source_key in self._schema_cache:
            del self._schema_cache[source_key]

        logger.debug(f"Cleared source {source_key}")

    def clear_all(self) -> None:
        """Clear all data."""
        self._arrow_batches.clear()
        self._polars_store.clear()
        self._entry_index.clear()
        self._schema_cache.clear()
        logger.info("Cleared all data")

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive statistics."""
        total_records = sum(len(entries) for entries in self._entry_index.values())
        total_batched = sum(len(batch) for batch in self._arrow_batches.values())
        total_consolidated = (
            sum(len(df) for df in self._polars_store.values())
            if self._polars_store
            else 0
        )

        source_stats = []
        for source_key in self._entry_index.keys():
            record_count = len(self._entry_index[source_key])
            batched_count = len(self._arrow_batches.get(source_key, []))
            consolidated_count = 0

            if source_key in self._polars_store:
                consolidated_count = len(self._polars_store[source_key])

            source_stats.append(
                {
                    "source_key": source_key,
                    "total_records": record_count,
                    "batched_records": batched_count,
                    "consolidated_records": consolidated_count,
                }
            )

        return {
            "total_records": total_records,
            "total_sources": len(self._entry_index),
            "total_batched": total_batched,
            "total_consolidated": total_consolidated,
            "batch_size": self.batch_size,
            "duplicate_entry_behavior": self.duplicate_entry_behavior,
            "source_details": source_stats,
        }

    def optimize_for_reads(self) -> None:
        """Optimize for read operations by consolidating all batches."""
        logger.info("Optimizing for reads - consolidating all Arrow batches...")
        self.force_consolidation()
        # Clear Arrow batches to save memory
        self._arrow_batches.clear()
        logger.info("Optimization complete")
