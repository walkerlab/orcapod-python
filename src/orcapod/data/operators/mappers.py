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
from orcapod.data.operators.base import UnaryOperator

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class MapPackets(UnaryOperator):
    """
    Operator that maps packets in a stream using a user-defined function.
    The function is applied to each packet in the stream, and the resulting packets
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = True, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def op_forward(self, stream: dp.Stream) -> dp.Stream:
        tag_columns, packet_columns = stream.keys()

        if not any(n in packet_columns for n in self.name_map):
            # nothing to rename in the packet, return stream as is
            return stream

        table = stream.as_table(include_source=True)

        name_map = {tc: tc for tc in tag_columns}  # no renaming on tag columns
        for c in packet_columns:
            if c in self.name_map:
                name_map[c] = self.name_map[c]
                name_map[f"{constants.SOURCE_PREFIX}{c}"] = (
                    f"{constants.SOURCE_PREFIX}{self.name_map[c]}"
                )
            else:
                name_map[c] = c

        renamed_table = table.rename_columns(name_map)
        return ImmutableTableStream(
            renamed_table, tag_columns=tag_columns, source=self, upstreams=(stream,)
        )

    def op_validate_inputs(self, stream: dp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # verify that renamed value does NOT collide with other columns
        tag_columns, packet_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in packet_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_packet_columns = set(packet_columns) - set(relevant_source)
        overlapping_packet_columns = remaining_packet_columns.intersection(
            relevant_target
        )
        overlapping_tag_columns = set(tag_columns).intersection(relevant_target)

        if overlapping_packet_columns or overlapping_tag_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_packet_columns:
                message += f"overlapping packet columns: {overlapping_packet_columns}, "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
            raise InputValidationError(message)

    def op_output_types(self, stream: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        tag_typespec, packet_typespec = stream.types()

        # Create new packet typespec with renamed keys
        new_packet_typespec = {
            self.name_map.get(k, k): v for k, v in packet_typespec.items()
        }

        return tag_typespec, new_packet_typespec

    def op_identity_structure(self, stream: dp.Stream | None = None) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        ) + ((stream,) if stream is not None else ())


class MapTags(UnaryOperator):
    """
    Operator that maps tags in a stream using a user-defined function.
    The function is applied to each tag in the stream, and the resulting tags
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = True, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def op_forward(self, stream: dp.Stream) -> dp.Stream:
        tag_columns, packet_columns = stream.keys()

        if not any(n in tag_columns for n in self.name_map):
            # nothing to rename in the tags, return stream as is
            return stream

        table = stream.as_table(include_source=True)

        name_map = {
            tc: self.name_map.get(tc, tc) for tc in tag_columns
        }  # rename the tag as necessary
        new_tag_columns = [name_map[tc] for tc in tag_columns]
        for c in packet_columns:
            name_map[c] = c  # no renaming on packet columns

        renamed_table = table.rename_columns(name_map)
        return ImmutableTableStream(
            renamed_table, tag_columns=new_tag_columns, source=self, upstreams=(stream,)
        )

    def op_validate_inputs(self, stream: dp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # verify that renamed value does NOT collide with other columns
        tag_columns, packet_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in tag_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_tag_columns = set(tag_columns) - set(relevant_source)
        overlapping_tag_columns = remaining_tag_columns.intersection(relevant_target)
        overlapping_packet_columns = set(packet_columns).intersection(relevant_target)

        if overlapping_tag_columns or overlapping_packet_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
            if overlapping_packet_columns:
                message += f"overlapping packet columns: {overlapping_packet_columns}."
            raise InputValidationError(message)

    def op_output_types(self, stream: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        tag_typespec, packet_typespec = stream.types()

        # Create new packet typespec with renamed keys
        new_tag_typespec = {self.name_map.get(k, k): v for k, v in tag_typespec.items()}

        return new_tag_typespec, packet_typespec

    def op_identity_structure(self, stream: dp.Stream | None = None) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        ) + ((stream,) if stream is not None else ())
