# Collection of functions to work with Arrow table data that underlies streams and/or datagrams
from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING
from orcapod.core.system_constants import constants
from collections.abc import Collection

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def drop_columns_with_prefix(
    table: "pa.Table",
    prefix: str | tuple[str, ...],
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
    """Drop columns with a specific prefix from an Arrow table."""
    columns_to_drop = [
        col
        for col in table.column_names
        if col.startswith(prefix) and col not in exclude_columns
    ]
    return table.drop(columns=columns_to_drop)


def drop_system_columns(
    table,
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> "pa.Table":
    return drop_columns_with_prefix(table, system_column_prefix)


def add_source_info(
    table: "pa.Table",
    source_info: str | Collection[str] | None,
    exclude_prefixes: Collection[str] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
    """Add source information to an Arrow table."""
    # Create a new column with the source information
    if source_info is None or isinstance(source_info, str):
        source_column = [source_info] * table.num_rows
    elif isinstance(source_info, Collection):
        if len(source_info) != table.num_rows:
            raise ValueError(
                "Length of source_info collection must match number of rows in the table."
            )
        source_column = source_info

    # identify columns for which source columns should be created

    for col in table.column_names:
        if col.startswith(tuple(exclude_prefixes)) or col in exclude_columns:
            continue
        source_column = pa.array(
            [f"{source_val}::{col}" for source_val in source_column],
            type=pa.large_string(),
        )
        table = table.append_column(f"{constants.SOURCE_PREFIX}{col}", source_column)

    return table
