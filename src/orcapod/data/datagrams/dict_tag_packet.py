import logging
from collections.abc import Collection, Mapping
from typing import Self
from xml.etree.ElementInclude import include

import pyarrow as pa

from orcapod.data.system_constants import orcapod_constants as constants
from orcapod.data.context import DataContext
from orcapod.data.datagrams.dict_datagram import DictDatagram
from orcapod.types import TypeSpec, schemas
from orcapod.types.core import DataValue
from orcapod.types.semantic_converter import SemanticConverter
from orcapod.utils import arrow_utils

logger = logging.getLogger(__name__)


class DictTag(DictDatagram):
    """
    A simple tag implementation using Python dictionary.

    Represents a tag (metadata) as a dictionary that can be converted
    to different representations like Arrow tables.
    """


class DictPacket(DictDatagram):
    """
    Enhanced packet implementation with source information support.

    Extends DictDatagram to include source information tracking and
    enhanced table conversion capabilities that can include or exclude
    source metadata.

    Initialize packet with data and optional source information.

    Args:
        data: Primary data content
        source_info: Optional mapping of field names to source information
        typespec: Optional type specification
        semantic_converter: Optional semantic converter
        semantic_type_registry: Registry for semantic types. Defaults to system default registry.
        arrow_hasher: Optional Arrow hasher. Defaults to system default arrow hasher.
    """

    def __init__(
        self,
        data: Mapping[str, DataValue],
        meta_info: Mapping[str, DataValue] | None = None,
        source_info: Mapping[str, str | None] | None = None,
        typespec: TypeSpec | None = None,
        semantic_converter: SemanticConverter | None = None,
        data_context: str | DataContext | None = None,
    ) -> None:
        # normalize the data content and remove any source info keys
        data_only = {
            k: v for k, v in data.items() if not k.startswith(constants.SOURCE_PREFIX)
        }
        contained_source_info = {
            k.removeprefix(constants.SOURCE_PREFIX): v
            for k, v in data.items()
            if k.startswith(constants.SOURCE_PREFIX)
        }

        super().__init__(
            data_only,
            typespec=typespec,
            meta_info=meta_info,
            semantic_converter=semantic_converter,
            data_context=data_context,
        )

        self._source_info = {**contained_source_info, **(source_info or {})}
        self._cached_source_info_table: pa.Table | None = None
        self._cached_source_info_schema: pa.Schema | None = None

    @property
    def _source_info_schema(self) -> pa.Schema:
        if self._cached_source_info_schema is None:
            self._cached_source_info_schema = pa.schema(
                {
                    f"{constants.SOURCE_PREFIX}{k}": pa.large_string()
                    for k in self.keys()
                }
            )
        return self._cached_source_info_schema

    def as_table(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> pa.Table:
        """Convert the packet to an Arrow table."""
        table = super().as_table(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            if self._cached_source_info_table is None:
                source_info_data = {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self.source_info().items()
                }
                self._cached_source_info_table = pa.Table.from_pylist(
                    [source_info_data], schema=self._source_info_schema
                )
            assert self._cached_source_info_table is not None, (
                "Cached source info table should not be None"
            )
            # subselect the corresponding _source_info as the columns present in the data table
            source_info_table = self._cached_source_info_table.select(
                [
                    f"{constants.SOURCE_PREFIX}{k}"
                    for k in table.column_names
                    if k in self.keys()
                ]
            )
            table = arrow_utils.hstack_tables(table, source_info_table)
        return table

    def as_dict(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> dict[str, DataValue]:
        """
        Return dictionary representation.

        Args:
            include_source: Whether to include source info fields

        Returns:
            Dictionary representation of the packet
        """
        dict_copy = super().as_dict(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            for key, value in self.source_info().items():
                dict_copy[f"{constants.SOURCE_PREFIX}{key}"] = value
        return dict_copy

    def keys(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> tuple[str, ...]:
        """Return keys of the Python schema."""
        keys = super().keys(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_context=include_context,
        )
        if include_all_info or include_source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{key}" for key in super().keys())
        return keys

    def types(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_context: bool = False,
        include_source: bool = False,
    ) -> schemas.PythonSchema:
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
            return arrow_utils.join_arrow_schemas(schema, self._source_info_schema)
        return schema

    def as_datagram(
        self,
        include_all_info: bool = False,
        include_meta_columns: bool | Collection[str] = False,
        include_source: bool = False,
    ) -> DictDatagram:
        """
        Convert the packet to a DictDatagram.

        Args:
            include_source: Whether to include source info fields

        Returns:
            DictDatagram representation of the packet
        """

        data = self.as_dict(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_source=include_source,
        )
        typespec = self.types(
            include_all_info=include_all_info,
            include_meta_columns=include_meta_columns,
            include_source=include_source,
        )
        return DictDatagram(
            data,
            typespec=typespec,
            semantic_converter=self._semantic_converter,
            data_context=self._data_context,
        )

    def source_info(self) -> dict[str, str | None]:
        """
        Return source information for all keys.

        Returns:
            Dictionary mapping field names to their source info
        """
        return {key: self._source_info.get(key, None) for key in self.keys()}

    def copy(self, include_cache: bool = True) -> Self:
        """Return a shallow copy of the packet."""
        instance = super().copy(include_cache=include_cache)
        instance._source_info = self._source_info.copy()
        if include_cache:
            instance._cached_source_info_table = self._cached_source_info_table
        return instance
