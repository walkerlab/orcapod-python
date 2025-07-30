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

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class Operator(TrackedKernelBase):
    """
    Base class for all operators.
    Operators are a special type of kernel that can be used to perform operations on streams.

    They are defined as a callable that takes a (possibly empty) collection of streams as the input
    and returns a new stream as output (note that output stream is always singular).
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._operator_hash = self.data_context.object_hasher.hash_to_hex(
            self.identity_structure(), prefix_hasher_id=True
        )

    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}", self._operator_hash)


class NonZeroInputOperator(Operator):
    """
    Operators that work with at least one input stream.
    This is useful for operators that can take a variable number of (but at least one ) input streams,
    such as joins, unions, etc.
    """

    def verify_non_zero_input(
        self,
        streams: Collection[dp.Stream],
    ) -> None:
        """
        Check that the inputs to the variable inputs operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        if len(streams) == 0:
            raise ValueError(
                f"Operator {self.__class__.__name__} requires at least one input stream."
            )

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.verify_non_zero_input(streams)
        return self.op_validate_inputs(*streams)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Forward method for variable inputs operators.
        It expects at least one stream as input.
        """
        return self.op_forward(*streams)

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        return self.op_output_types(*streams)

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        """
        Return a structure that represents the identity of this operator.
        This is used to ensure that the operator can be uniquely identified in the computational graph.
        """
        return self.op_identity_structure(streams)

    @abstractmethod
    def op_validate_inputs(self, *streams: dp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    @abstractmethod
    def op_forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        This method should be implemented by subclasses to define the specific behavior of the non-zero input operator.
        It takes variable number of streams as input and returns a new stream as output.
        """
        ...

    @abstractmethod
    def op_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes at least one stream as input and returns a tuple of typespecs.
        """
        ...

    @abstractmethod
    def op_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        """
        This method should be implemented by subclasses to return a structure that represents the identity of the operator.
        It takes zero or more streams as input and returns a tuple containing the operator name and a set of streams.
        If zero, it should return identity of the operator itself.
        If one or more, it should return a identity structure approrpiate for the operator invoked on the given streams.
        """
        ...


class BinaryOperator(Operator):
    """
    Base class for all operators.
    """

    def check_binary_inputs(
        self,
        streams: Collection[dp.Stream],
    ) -> None:
        """
        Check that the inputs to the binary operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        if len(streams) != 2:
            raise ValueError("BinaryOperator requires exactly two input streams.")

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.check_binary_inputs(streams)
        left_stream, right_stream = streams
        return self.op_validate_inputs(left_stream, right_stream)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Forward method for binary operators.
        It expects exactly two streams as input.
        """
        left_stream, right_stream = streams
        return self.op_forward(left_stream, right_stream)

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        left_stream, right_stream = streams
        return self.op_output_types(left_stream, right_stream)

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        """
        Return a structure that represents the identity of this operator.
        This is used to ensure that the operator can be uniquely identified in the computational graph.
        """
        if streams is not None:
            left_stream, right_stream = streams
            self.op_identity_structure(left_stream, right_stream)
        return self.op_identity_structure()

    @abstractmethod
    def op_validate_inputs(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    @abstractmethod
    def op_forward(self, left_stream: dp.Stream, right_stream: dp.Stream) -> dp.Stream:
        """
        This method should be implemented by subclasses to define the specific behavior of the binary operator.
        It takes two streams as input and returns a new stream as output.
        """
        ...

    @abstractmethod
    def op_output_types(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> tuple[TypeSpec, TypeSpec]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes two streams as input and returns a tuple of typespecs.
        """
        ...

    @abstractmethod
    def op_identity_structure(
        self,
        left_stream: dp.Stream | None = None,
        right_stream: dp.Stream | None = None,
    ) -> Any:
        """
        This method should be implemented by subclasses to return a structure that represents the identity of the operator.
        It takes two streams as input and returns a tuple containing the operator name and a set of streams.
        """
        ...


class UnaryOperator(Operator):
    """
    Base class for all operators.
    """

    def check_unary_input(
        self,
        streams: Collection[dp.Stream],
    ) -> None:
        """
        Check that the inputs to the unary operator are valid.
        """
        if len(streams) != 1:
            raise ValueError("UnaryOperator requires exactly one input stream.")

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.check_unary_input(streams)
        stream = streams[0]
        return self.op_validate_inputs(stream)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Forward method for unary operators.
        It expects exactly one stream as input.
        """
        stream = streams[0]
        return self.op_forward(stream)

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        stream = streams[0]
        return self.op_output_types(stream)

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        """
        Return a structure that represents the identity of this operator.
        This is used to ensure that the operator can be uniquely identified in the computational graph.
        """
        if streams is not None:
            stream = list(streams)[0]
            self.op_identity_structure(stream)
        return self.op_identity_structure()

    @abstractmethod
    def op_validate_inputs(self, stream: dp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    @abstractmethod
    def op_forward(self, stream: dp.Stream) -> dp.Stream:
        """
        This method should be implemented by subclasses to define the specific behavior of the binary operator.
        It takes two streams as input and returns a new stream as output.
        """
        ...

    @abstractmethod
    def op_output_types(self, stream: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes two streams as input and returns a tuple of typespecs.
        """
        ...

    @abstractmethod
    def op_identity_structure(self, stream: dp.Stream | None = None) -> Any:
        """
        This method should be implemented by subclasses to return a structure that represents the identity of the operator.
        It takes two streams as input and returns a tuple containing the operator name and a set of streams.
        """
        ...


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


class SemiJoin(BinaryOperator):
    """
    Binary operator that performs a semi-join between two streams.

    A semi-join returns only the entries from the left stream that have
    matching entries in the right stream, based on equality of values
    in overlapping columns (columns with the same name and compatible types).

    If there are no overlapping columns between the streams, the entire
    left stream is returned unchanged.

    The output stream preserves the schema of the left stream exactly.
    """

    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def op_identity_structure(
        self,
        left_stream: dp.Stream | None = None,
        right_stream: dp.Stream | None = None,
    ) -> Any:
        """
        Return a structure that represents the identity of this operator.
        Unlike Join, SemiJoin depends on the order of streams (left vs right).
        """
        id_struct = (self.__class__.__name__,)
        if left_stream is not None and right_stream is not None:
            # Order matters for semi-join: (left_stream, right_stream)
            id_struct += (left_stream, right_stream)
        return id_struct

    def op_forward(self, left_stream: dp.Stream, right_stream: dp.Stream) -> dp.Stream:
        """
        Performs a semi-join between left and right streams.
        Returns entries from left stream that have matching entries in right stream.
        """
        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()

        # Find overlapping columns across all columns (tags + packets)
        left_all_typespec = union_typespecs(left_tag_typespec, left_packet_typespec)
        right_all_typespec = union_typespecs(right_tag_typespec, right_packet_typespec)

        common_keys = tuple(
            intersection_typespecs(left_all_typespec, right_all_typespec).keys()
        )

        # If no overlapping columns, return the left stream unmodified
        if not common_keys:
            return left_stream

        # include source info for left stream
        left_table = left_stream.as_table(include_source=True)

        # Get the right table for matching
        right_table = right_stream.as_table()

        # Perform left semi join using PyArrow's built-in functionality
        semi_joined_table = left_table.join(
            right_table,
            keys=list(common_keys),
            join_type="left semi",
        )

        return ImmutableTableStream(
            semi_joined_table,
            tag_columns=tuple(left_tag_typespec.keys()),
            source=self,
            upstreams=(left_stream, right_stream),
        )

    def op_output_types(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> tuple[TypeSpec, TypeSpec]:
        """
        Returns the output types for the semi-join operation.
        The output preserves the exact schema of the left stream.
        """
        # Semi-join preserves the left stream's schema exactly
        return left_stream.types()

    def op_validate_inputs(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> None:
        """
        Validates that the input streams are compatible for semi-join.
        Checks that overlapping columns have compatible types.
        """
        try:
            left_tag_typespec, left_packet_typespec = left_stream.types()
            right_tag_typespec, right_packet_typespec = right_stream.types()

            # Check that overlapping columns have compatible types across all columns
            left_all_typespec = union_typespecs(left_tag_typespec, left_packet_typespec)
            right_all_typespec = union_typespecs(
                right_tag_typespec, right_packet_typespec
            )

            # intersection_typespecs will raise an error if types are incompatible
            intersection_typespecs(left_all_typespec, right_all_typespec)

        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for semi-join: {e}"
            ) from e

    def __repr__(self) -> str:
        return "SemiJoin()"


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
