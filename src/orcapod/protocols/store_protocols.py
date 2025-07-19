from typing import Collection, Protocol, TYPE_CHECKING
from orcapod.protocols import data_protocols as dp
import pyarrow as pa

if TYPE_CHECKING:
    import polars as pl


class ArrowDataStore(Protocol):
    def record_data(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        data: pa.Table,
        ignore_duplicates: bool | None = None,
    ) -> str | None: ...

    def get_recorded_data(
        self,
        record_path: tuple[str, ...],
        record_id: str,
    ) -> pa.Table | None: ...

    def get_all_records(self, record_path: tuple[str, ...]) -> pa.Table | None:
        """Retrieve all records for a given path as a stream."""
        ...

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        add_entry_id_column: bool | str = False,
        preseve_input_order: bool = False,
    ) -> pa.Table: ...
