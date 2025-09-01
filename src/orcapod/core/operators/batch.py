from orcapod.core.operators.base import UnaryOperator
from collections.abc import Collection
from orcapod.protocols import core_protocols as cp
from typing import Any, TYPE_CHECKING
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams import TableStream

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

from orcapod.types import PythonSchema


class Batch(UnaryOperator):
    """
    Base class for all operators.
    """

    def __init__(self, batch_size: int = 0, drop_partial_batch: bool = False, **kwargs):
        if batch_size < 0:
            raise ValueError("Batch size must be non-negative.")

        super().__init__(**kwargs)

        self.batch_size = batch_size
        self.drop_partial_batch = drop_partial_batch

    def check_unary_input(
        self,
        streams: Collection[cp.Stream],
    ) -> None:
        """
        Check that the inputs to the unary operator are valid.
        """
        if len(streams) != 1:
            raise ValueError("UnaryOperator requires exactly one input stream.")

    def validate_inputs(self, *streams: cp.Stream) -> None:
        self.check_unary_input(streams)
        stream = streams[0]
        return self.op_validate_inputs(stream)

    def op_validate_inputs(self, stream: cp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        return None

    def op_forward(self, stream: cp.Stream) -> cp.Stream:
        """
        This method should be implemented by subclasses to define the specific behavior of the binary operator.
        It takes two streams as input and returns a new stream as output.
        """
        table = stream.as_table(include_source=True, include_system_tags=True)

        tag_columns, packet_columns = stream.keys()

        data_list = table.to_pylist()

        batched_data = []

        next_batch = {}

        i = 0
        for entry in data_list:
            i += 1
            for c in entry:
                next_batch.setdefault(c, []).append(entry[c])

            if self.batch_size > 0 and i >= self.batch_size:
                batched_data.append(next_batch)
                next_batch = {}
                i = 0

        if i > 0 and not self.drop_partial_batch:
            batched_data.append(next_batch)

        batched_table = pa.Table.from_pylist(batched_data)
        return TableStream(batched_table, tag_columns=tag_columns)

    def op_output_types(
        self, stream: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes two streams as input and returns a tuple of typespecs.
        """
        tag_types, packet_types = stream.types(include_system_tags=include_system_tags)
        batched_tag_types = {k: list[v] for k, v in tag_types.items()}
        batched_packet_types = {k: list[v] for k, v in packet_types.items()}

        # TODO: check if this is really necessary
        return PythonSchema(batched_tag_types), PythonSchema(batched_packet_types)

    def op_identity_structure(self, stream: cp.Stream | None = None) -> Any:
        return (
            (self.__class__.__name__, self.batch_size, self.drop_partial_batch)
            + (stream,)
            if stream is not None
            else ()
        )
