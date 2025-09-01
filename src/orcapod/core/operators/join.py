from orcapod.protocols import core_protocols as cp
from orcapod.core.streams import TableStream
from orcapod.types import PythonSchema
from orcapod.utils import types_utils
from typing import Any, TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from collections.abc import Collection
from orcapod.errors import InputValidationError
from orcapod.core.operators.base import NonZeroInputOperator
from orcapod.core import arrow_data_utils

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class Join(NonZeroInputOperator):
    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def op_validate_inputs(self, *streams: cp.Stream) -> None:
        try:
            self.op_output_types(*streams)
        except Exception as e:
            # raise InputValidationError(f"Input streams are not compatible: {e}") from e
            raise e

    def order_input_streams(self, *streams: cp.Stream) -> list[cp.Stream]:
        # order the streams based on their hashes to offer deterministic operation
        return sorted(streams, key=lambda s: s.content_hash().to_hex())

    def op_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        if len(streams) == 1:
            # If only one stream is provided, return its typespecs
            return streams[0].types(include_system_tags=include_system_tags)

        # output type computation does NOT require consistent ordering of streams

        # TODO: consider performing the check always with system tags on
        stream = streams[0]
        tag_typespec, packet_typespec = stream.types(
            include_system_tags=include_system_tags
        )
        for other_stream in streams[1:]:
            other_tag_typespec, other_packet_typespec = other_stream.types(
                include_system_tags=include_system_tags
            )
            tag_typespec = types_utils.union_typespecs(tag_typespec, other_tag_typespec)
            intersection_packet_typespec = types_utils.intersection_typespecs(
                packet_typespec, other_packet_typespec
            )
            packet_typespec = types_utils.union_typespecs(
                packet_typespec, other_packet_typespec
            )
            if intersection_packet_typespec:
                raise InputValidationError(
                    f"Packets should not have overlapping keys, but {packet_typespec.keys()} found in {stream} and {other_stream}."
                )

        return tag_typespec, packet_typespec

    def op_forward(self, *streams: cp.Stream) -> cp.Stream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) == 1:
            return streams[0]

        COMMON_JOIN_KEY = "_common"

        stream = streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        table = stream.as_table(include_source=True, include_system_tags=True)
        # trick to get cartesian product
        table = table.add_column(0, COMMON_JOIN_KEY, pa.array([0] * len(table)))
        table = arrow_data_utils.append_to_system_tags(
            table,
            stream.content_hash().to_hex(self.orcapod_config.system_tag_hash_n_char),
        )

        for next_stream in streams[1:]:
            next_tag_keys, _ = next_stream.keys()
            next_table = next_stream.as_table(
                include_source=True, include_system_tags=True
            )
            next_table = arrow_data_utils.append_to_system_tags(
                next_table,
                next_stream.content_hash().to_hex(
                    char_count=self.orcapod_config.system_tag_hash_n_char
                ),
            )
            # trick to ensure that there will always be at least one shared key
            # this ensure that no overlap in keys lead to full caretesian product
            next_table = next_table.add_column(
                0, COMMON_JOIN_KEY, pa.array([0] * len(next_table))
            )
            common_tag_keys = tag_keys.intersection(next_tag_keys)
            common_tag_keys.add(COMMON_JOIN_KEY)

            table = (
                pl.DataFrame(table)
                .join(pl.DataFrame(next_table), on=list(common_tag_keys), how="inner")
                .to_arrow()
            )

            tag_keys.update(next_tag_keys)

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(COMMON_JOIN_KEY)
        reordered_columns = [col for col in table.column_names if col in tag_keys]
        reordered_columns += [col for col in table.column_names if col not in tag_keys]

        return TableStream(
            table.select(reordered_columns),
            tag_columns=tuple(tag_keys),
            source=self,
            upstreams=streams,
        )

    def op_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        return (
            (self.__class__.__name__,) + (set(streams),) if streams is not None else ()
        )

    def __repr__(self) -> str:
        return "Join()"
