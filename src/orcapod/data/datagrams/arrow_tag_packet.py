import logging
from collections.abc import Collection, Mapping
from typing import Self


import pyarrow as pa

from orcapod.data.system_constants import orcapod_constants as constants
from orcapod import contexts
from orcapod.types import TypeSpec

from orcapod.types.core import DataValue
from orcapod.utils import arrow_utils

from orcapod.data.datagrams.arrow_datagram import ArrowDatagram

logger = logging.getLogger(__name__)


class ArrowTag(ArrowDatagram):
    """
    A tag implementation using Arrow table backend.

    Represents a single-row Arrow table that can be converted to Python
    dictionary representation while caching computed values for efficiency.

    Initialize with an Arrow table.

        Args:
            table: Single-row Arrow table representing the tag

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        table: pa.Table,
        system_tags: Mapping[str, DataValue] | None = None,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowTag should only contain a single row, "
                "as it represents a single tag."
            )
        super().__init__(
            table=table,
            data_context=data_context,
        )
        extracted_system_tags = [
            c for c in self._data_table.column_names if c.startswith("_tag_")
        ]
        self._system_tag_table = self._data_table.select(extracted_system_tags)
        self._data_table = self._data_table.drop_columns(extracted_system_tags)


class ArrowPacket(ArrowDatagram):
    """
    Arrow table-based packet implementation with comprehensive features.

    A packet implementation that uses Arrow tables as the primary storage format,
    providing efficient memory usage and columnar data operations while supporting
    source information tracking and content hashing.


    Initialize ArrowPacket with Arrow table and configuration.

        Args:
            table: Single-row Arrow table representing the packet
            source_info: Optional source information mapping
            semantic_converter: Optional semantic converter
            semantic_type_registry: Registry for semantic types
            finger_print: Optional fingerprint for tracking
            arrow_hasher: Optional Arrow hasher
            post_hash_callback: Optional callback after hash calculation
            skip_source_info_extraction: Whether to skip source info processing

        Raises:
            ValueError: If table doesn't contain exactly one row
    """

    def __init__(
        self,
        table: pa.Table | pa.RecordBatch,
        meta_info: Mapping[str, DataValue] | None = None,
        source_info: Mapping[str, str | None] | None = None,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "ArrowPacket should only contain a single row, "
                "as it represents a single packet."
            )
        if source_info is None:
            source_info = {}
        else:
            # normalize by removing any existing prefixes
            source_info = {
                (
                    k.removeprefix(constants.SOURCE_PREFIX)
                    if k.startswith(constants.SOURCE_PREFIX)
                    else k
                ): v
                for k, v in source_info.items()
            }

        # normalize the table to ensure it has the expected source_info columns
        # TODO: use simpler function to ensure source_info columns
        data_table, prefixed_tables = arrow_utils.prepare_prefixed_columns(
            table,
            {constants.SOURCE_PREFIX: source_info},
            exclude_columns=[constants.CONTEXT_KEY],
            exclude_prefixes=[constants.META_PREFIX],
        )

        super().__init__(
            data_table,
            meta_info=meta_info,
            data_context=data_context,
        )
        self._source_info_table = prefixed_tables[constants.SOURCE_PREFIX]

        self._cached_source_info: dict[str, str | None] | None = None
        self._cached_python_schema: TypeSpec | None = None
        self._cached_content_hash: str | None = None

    def keys(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{k}" for k in self.keys())
        return keys

    def types(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> dict[str, type]:
        """Return copy of the Python schema."""
        schema = super().types(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            for key in self.keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = str
        return schema

    def arrow_schema(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> pa.Schema:
        """
        Return the PyArrow schema for this datagram.

        Args:
            include_data_context: Whether to include data context column in the schema
            include_source: Whether to include source info columns in the schema

        Returns:
            PyArrow schema representing the datagram's structure
        """
        schema = super().arrow_schema(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            return arrow_utils.join_arrow_schemas(
                schema, self._source_info_table.schema
            )
        return schema

    def as_dict(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> dict[str, DataValue]:
        """
        Convert to dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        return_dict = super().as_dict(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            return_dict.update(
                {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
            )
        return return_dict

    def as_table(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> pa.Table:
        table = super().as_table(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            # add source_info only for existing data columns
            table = arrow_utils.hstack_tables(table, self._source_info_table)
        return table

    def as_datagram(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_source: bool = False,
    ) -> ArrowDatagram:
        table = self.as_table(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_source=include_source,
        )
        return ArrowDatagram(
            table,
            data_context=self._data_context,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Copy of the dictionary mapping field names to their source info
        """
        if self._cached_source_info is None:
            self._cached_source_info = {
                k.removeprefix(constants.SOURCE_PREFIX): v
                for k, v in self._source_info_table.to_pylist()[0].items()
            }
        return self._cached_source_info.copy()

    def with_source_info(self, **source_info: str | None) -> Self:
        """
        Create a copy of the packet with updated source information.

        Args:
            source_info: New source information mapping

        Returns:
            New ArrowPacket instance with updated source info
        """
        new_packet = self.copy(include_cache=False)

        existing_source_info_with_prefix = self._source_info_table.to_pylist()[0]
        for key, value in source_info.items():
            if not key.startswith(constants.SOURCE_PREFIX):
                # Ensure the key is prefixed correctly
                key = f"{constants.SOURCE_PREFIX}{key}"
            if key in existing_source_info_with_prefix:
                existing_source_info_with_prefix[key] = value

        new_packet._source_info_table = pa.Table.from_pylist(
            [existing_source_info_with_prefix]
        )
        return new_packet

    # 8. Utility Operations
    def copy(self, include_cache: bool = True) -> Self:
        """Return a copy of the datagram."""
        new_packet = super().copy(include_cache=include_cache)

        if include_cache:
            new_packet._cached_source_info = self._cached_source_info
        else:
            new_packet._cached_source_info = None

        return new_packet
