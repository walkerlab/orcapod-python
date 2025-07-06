from typing import Protocol, runtime_checkable

from orcapod.types import Tag, PacketLike
import pyarrow as pa
import polars as pl


class DuplicateError(ValueError):
    pass


@runtime_checkable
class DataStore(Protocol):
    """
    Protocol for data stores that can memoize and retrieve packets.
    This is used to define the interface for data stores like DirDataStore.
    """

    def __init__(self, *args, **kwargs) -> None: ...
    def memoize(
        self,
        function_name: str,
        function_hash: str,
        packet: PacketLike,
        output_packet: PacketLike,
    ) -> PacketLike: ...

    def retrieve_memoized(
        self, function_name: str, function_hash: str, packet: PacketLike
    ) -> PacketLike | None: ...


@runtime_checkable
class ArrowDataStore(Protocol):
    """
    Protocol for data stores that can memoize and retrieve packets.
    This is used to define the interface for data stores like DirDataStore.
    """

    def __init__(self, *args, **kwargs) -> None: ...

    def add_record(
        self,
        source_path: tuple[str, ...],
        entry_id: str,
        arrow_data: pa.Table,
        ignore_duplicate: bool = False,
    ) -> pa.Table: ...

    def get_record(
        self, source_path: tuple[str, ...], entry_id: str
    ) -> pa.Table | None: ...

    def get_all_records(self, source_path: tuple[str, ...]) -> pa.Table | None:
        """Retrieve all records for a given source as a single table."""
        ...

    def get_all_records_as_polars(
        self, source_path: tuple[str, ...]
    ) -> pl.LazyFrame | None:
        """Retrieve all records for a given source as a single Polars DataFrame."""
        ...

    def get_records_by_ids(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pa.Table | None:
        """Retrieve records by entry IDs as a single table."""
        ...

    def get_records_by_ids_as_polars(
        self,
        source_path: tuple[str, ...],
        entry_ids: list[str] | pl.Series | pa.Array,
        add_entry_id_column: bool | str = False,
        preserve_input_order: bool = False,
    ) -> pl.LazyFrame | None:
        """Retrieve records by entry IDs as a single Polars DataFrame."""
        ...

    def flush(self) -> None:
        """Flush all pending writes/saves to the data store."""
        ...
