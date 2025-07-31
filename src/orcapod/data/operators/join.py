from orcapod.data.kernels import TrackedKernelBase
from orcapod.protocols import data_protocols as dp
from orcapod.data.streams import ImmutableTableStream
from orcapod.types import TypeSpec
from orcapod.types.typespec_utils import union_typespecs, intersection_typespecs
from abc import abstractmethod
from typing import Any, TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from collections.abc import Collection, Mapping
from orcapod.errors import InputValidationError
from orcapod.data.system_constants import orcapod_constants as constants
from orcapod.data.operators.base import NonZeroInputOperator

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class Join(NonZeroInputOperator):
    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def op_validate_inputs(self, *streams: dp.Stream) -> None:
        try:
            self.op_output_types(*streams)
        except Exception as e:
            # raise InputValidationError(f"Input streams are not compatible: {e}") from e
            raise e

    def op_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        if len(streams) == 1:
            # If only one stream is provided, return its typespecs
            return streams[0].types()

        stream = streams[0]
        tag_typespec, packet_typespec = stream.types()
        for other_stream in streams[1:]:
            other_tag_typespec, other_packet_typespec = other_stream.types()
            tag_typespec = union_typespecs(tag_typespec, other_tag_typespec)
            packet_typespec = intersection_typespecs(
                packet_typespec, other_packet_typespec
            )
            if packet_typespec:
                raise InputValidationError(
                    f"Packets should not have overlapping keys, but {packet_typespec.keys()} found in {stream} and {other_stream}."
                )

        return tag_typespec, packet_typespec

    def op_forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) == 1:
            return streams[0]

        COMMON_JOIN_KEY = "_common"

        stream = streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        table = stream.as_table(include_source=True)
        # trick to get cartesian product
        table = table.add_column(0, COMMON_JOIN_KEY, pa.array([0] * len(table)))

        for next_stream in streams[1:]:
            next_tag_keys, _ = next_stream.keys()
            next_table = next_stream.as_table(include_source=True)
            next_table = next_table.add_column(
                0, COMMON_JOIN_KEY, pa.array([0] * len(next_table))
            )
            common_tag_keys = tag_keys.intersection(next_tag_keys)
            common_tag_keys.add(COMMON_JOIN_KEY)

            table = table.join(
                next_table, keys=list(common_tag_keys), join_type="inner"
            )
            tag_keys.update(next_tag_keys)

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(COMMON_JOIN_KEY)
        reordered_columns = [col for col in table.column_names if col in tag_keys]
        reordered_columns += [col for col in table.column_names if col not in tag_keys]

        return ImmutableTableStream(
            table.select(reordered_columns),
            tag_columns=tuple(tag_keys),
            source=self,
            upstreams=streams,
        )

    def op_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        return (
            (self.__class__.__name__,) + (set(streams),) if streams is not None else ()
        )

    def __repr__(self) -> str:
        return "Join()"
