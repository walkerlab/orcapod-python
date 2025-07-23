from typing import Protocol
from collections.abc import Collection
import pyarrow as pa


class ArrowDataStore(Protocol):
    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        data: pa.Table,
        ignore_duplicates: bool | None = None,
        overwrite_existing: bool = False,
    ) -> str | None: ...

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        ignore_duplicates: bool | None = None,
        overwrite_existing: bool = False,
    ) -> list[str]: ...

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
    ) -> pa.Table | None: ...

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> pa.Table | None:
        """Retrieve all records for a given path as a stream."""
        ...

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
    ) -> pa.Table: ...

    def flush(self) -> None:
        """Flush any buffered writes to the underlying storage."""
        ...
