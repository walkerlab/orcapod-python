from orcapod.data.kernels import TrackedKernelBase
from orcapod.protocols import data_protocols as dp
from orcapod.data.streams import ImmutableTableStream
from orcapod.types import TypeSpec
from orcapod.types.typespec_utils import union_typespecs, intersection_typespecs
from abc import abstractmethod
from typing import Any


class InputValidationError(Exception):
    """
    Exception raised when the inputs are not valid.
    This is used to indicate that the inputs do not meet the requirements of the operator.
    """


class Operator(TrackedKernelBase):
    """
    Base class for all operators.
    Operators are a special type of kernel that can be used to perform operations on streams.
    They are defined as a callable that takes a (possibly empty) collection of streams as the input
    and returns a new stream as output (note that output stream is always singular).
    """


class NonZeroInputOperator(Operator):
    """
    Operators that work with at least one input stream.
    This is useful for operators that can take a variable number of (but at least one ) input streams,
    such as joins, unions, etc.
    """

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.verify_non_zero_input(*streams)
        return self.op_validate_inputs(*streams)

    @abstractmethod
    def op_validate_inputs(self, *streams: dp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    def verify_non_zero_input(
        self,
        *streams: dp.Stream,
    ) -> None:
        """
        Check that the inputs to the variable inputs operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        if len(streams) == 0:
            raise ValueError(
                f"Operator {self.__class__.__name__} requires at least one input stream."
            )

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Forward method for variable inputs operators.
        It expects at least one stream as input.
        """
        return self.op_forward(*streams)

    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        self.validate_inputs(*streams)
        return self.op_output_types(*streams)

    def identity_structure(self, *streams: dp.Stream) -> Any:
        """
        Return a structure that represents the identity of this operator.
        This is used to ensure that the operator can be uniquely identified in the computational graph.
        """
        if len(streams) > 0:
            self.verify_non_zero_input(*streams)
        return self.op_identity_structure(*streams)

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
    def op_identity_structure(self, *streams: dp.Stream) -> Any:
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

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.check_binary_inputs(*streams)
        left_stream, right_stream = streams
        return self.op_validate_inputs(left_stream, right_stream)

    @abstractmethod
    def op_validate_inputs(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    def check_binary_inputs(
        self, *streams: dp.Stream, allow_zero: bool = False
    ) -> None:
        """
        Check that the inputs to the binary operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        if not (allow_zero and len(streams) == 0) and len(streams) != 2:
            raise ValueError("BinaryOperator requires exactly two input streams.")

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Forward method for binary operators.
        It expects exactly two streams as input.
        """
        self.check_binary_inputs(*streams)
        left_stream, right_stream = streams
        return self.op_forward(left_stream, right_stream)

    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        self.check_binary_inputs(*streams)
        left_stream, right_stream = streams
        return self.op_output_types(left_stream, right_stream)

    def identity_structure(self, *streams: dp.Stream) -> Any:
        """
        Return a structure that represents the identity of this operator.
        This is used to ensure that the operator can be uniquely identified in the computational graph.
        """
        self.check_binary_inputs(*streams, allow_zero=True)
        return self.op_identity_structure(*streams)

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
    def op_identity_structure(self, *streams: dp.Stream) -> Any:
        """
        This method should be implemented by subclasses to return a structure that represents the identity of the operator.
        It takes two streams as input and returns a tuple containing the operator name and a set of streams.
        """
        ...


class BinaryJoin(BinaryOperator):
    def op_identity_structure(self, *streams: dp.Stream) -> Any:
        # Join does not depend on the order of the streams -- convert it onto a set
        id_struct = (self.__class__.__name__,)
        if len(streams) == 2:
            id_struct += (set(streams),)
        return id_struct

    def op_forward(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> ImmutableTableStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """

        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()

        common_tag_keys = tuple(
            intersection_typespecs(left_tag_typespec, right_tag_typespec).keys()
        )
        joined_tag_keys = tuple(
            union_typespecs(left_tag_typespec, right_tag_typespec).keys()
        )

        # performing a check to ensure that packets are compatible
        union_typespecs(left_packet_typespec, right_packet_typespec)

        joined_table = left_stream.as_table().join(
            right_stream.as_table(),
            keys=common_tag_keys,
            join_type="inner",
        )

        return ImmutableTableStream(
            joined_table,
            tag_columns=tuple(joined_tag_keys),
            source=self,
            upstreams=(left_stream, right_stream),
        )

    def op_output_types(self, left_stream, right_stream) -> tuple[TypeSpec, TypeSpec]:
        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()
        joined_tag_typespec = union_typespecs(left_tag_typespec, right_tag_typespec)
        joined_packet_typespec = union_typespecs(
            left_packet_typespec, right_packet_typespec
        )
        return joined_tag_typespec, joined_packet_typespec

    def op_validate_inputs(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> None:
        try:
            self.op_output_types(left_stream, right_stream)
        except Exception as e:
            raise InputValidationError(f"Input streams are not compatible: {e}")

    def __repr__(self) -> str:
        return "Join()"


class Join(NonZeroInputOperator):
    def op_identity_structure(self, *streams: dp.Stream) -> Any:
        # Join does not depend on the order of the streams -- convert it onto a set
        id_struct = (self.__class__.__name__,)
        if len(streams) > 0:
            id_struct += (set(streams),)
        return id_struct

    def op_forward(self, *streams: dp.Stream) -> ImmutableTableStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """

        all_tag_typespecs = []
        all_packet_typespecs = []

        for stream in streams:
            tag_typespec, packet_typespec = stream.types()
            all_tag_typespecs.append(tag_typespec)
            all_packet_typespecs.append(packet_typespec)

        common_tag_keys = tuple(intersection_typespecs(*all_tag_typespecs).keys())
        joined_tag_keys = tuple(union_typespecs(*all_tag_typespecs).keys())

        # performing a check to ensure that packets are compatible
        union_typespecs(*all_packet_typespecs)

        joined_table = left_stream.as_table().join(
            right_stream.as_table(),
            keys=common_tag_keys,
            join_type="inner",
        )

        return ImmutableTableStream(
            joined_table,
            tag_columns=tuple(joined_tag_keys),
            source=self,
            upstreams=streams,
        )

    def op_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        left_stream, right_stream = streams
        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()
        joined_tag_typespec = union_typespecs(left_tag_typespec, right_tag_typespec)
        joined_packet_typespec = union_typespecs(
            left_packet_typespec, right_packet_typespec
        )
        return joined_tag_typespec, joined_packet_typespec

    def op_validate_inputs(
        self, left_stream: dp.Stream, right_stream: dp.Stream
    ) -> None:
        try:
            self.op_output_types(left_stream, right_stream)
        except Exception as e:
            raise InputValidationError(f"Input streams are not compatible: {e}")

    def __repr__(self) -> str:
        return "Join()"
