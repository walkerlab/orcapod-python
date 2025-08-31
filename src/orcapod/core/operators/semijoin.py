from orcapod.protocols import core_protocols as cp
from orcapod.core.streams import TableStream
from orcapod.utils import types_utils
from orcapod.types import PythonSchema
from typing import Any, TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from orcapod.errors import InputValidationError
from orcapod.core.operators.base import BinaryOperator

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


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
        left_stream: cp.Stream | None = None,
        right_stream: cp.Stream | None = None,
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

    def op_forward(self, left_stream: cp.Stream, right_stream: cp.Stream) -> cp.Stream:
        """
        Performs a semi-join between left and right streams.
        Returns entries from left stream that have matching entries in right stream.
        """
        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()

        # Find overlapping columns across all columns (tags + packets)
        left_all_typespec = types_utils.union_typespecs(
            left_tag_typespec, left_packet_typespec
        )
        right_all_typespec = types_utils.union_typespecs(
            right_tag_typespec, right_packet_typespec
        )

        common_keys = tuple(
            types_utils.intersection_typespecs(
                left_all_typespec, right_all_typespec
            ).keys()
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

        return TableStream(
            semi_joined_table,
            tag_columns=tuple(left_tag_typespec.keys()),
            source=self,
            upstreams=(left_stream, right_stream),
        )

    def op_output_types(
        self,
        left_stream: cp.Stream,
        right_stream: cp.Stream,
        include_system_tags: bool = False,
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Returns the output types for the semi-join operation.
        The output preserves the exact schema of the left stream.
        """
        # Semi-join preserves the left stream's schema exactly
        return left_stream.types(include_system_tags=include_system_tags)

    def op_validate_inputs(
        self, left_stream: cp.Stream, right_stream: cp.Stream
    ) -> None:
        """
        Validates that the input streams are compatible for semi-join.
        Checks that overlapping columns have compatible types.
        """
        try:
            left_tag_typespec, left_packet_typespec = left_stream.types()
            right_tag_typespec, right_packet_typespec = right_stream.types()

            # Check that overlapping columns have compatible types across all columns
            left_all_typespec = types_utils.union_typespecs(
                left_tag_typespec, left_packet_typespec
            )
            right_all_typespec = types_utils.union_typespecs(
                right_tag_typespec, right_packet_typespec
            )

            # intersection_typespecs will raise an error if types are incompatible
            types_utils.intersection_typespecs(left_all_typespec, right_all_typespec)

        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for semi-join: {e}"
            ) from e

    def __repr__(self) -> str:
        return "SemiJoin()"
