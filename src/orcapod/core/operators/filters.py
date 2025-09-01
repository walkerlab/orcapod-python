from orcapod.protocols import core_protocols as cp
from orcapod.core.streams import TableStream
from orcapod.types import PythonSchema
from typing import Any, TYPE_CHECKING, TypeAlias
from orcapod.utils.lazy_module import LazyModule
from collections.abc import Collection, Mapping
from orcapod.errors import InputValidationError
from orcapod.core.system_constants import constants
from orcapod.core.operators.base import UnaryOperator
import logging
from collections.abc import Iterable


if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    import polars._typing as pl_type
    import numpy as np
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pl_type = LazyModule("polars._typing")

logger = logging.getLogger(__name__)

polars_predicate: TypeAlias = "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"


class PolarsFilter(UnaryOperator):
    """
    Operator that applies Polars filtering to a stream
    """

    def __init__(
        self,
        predicates: Collection[
            "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"
        ] = (),
        constraints: Mapping[str, Any] | None = None,
        **kwargs,
    ):
        self.predicates = predicates
        self.constraints = constraints if constraints is not None else {}
        super().__init__(**kwargs)

    def op_forward(self, stream: cp.Stream) -> cp.Stream:
        if len(self.predicates) == 0 and len(self.constraints) == 0:
            logger.info(
                "No predicates or constraints specified. Returning stream unaltered."
            )
            return stream

        # TODO: improve efficiency here...
        table = stream.as_table(
            include_source=True, include_system_tags=True, sort_by_tags=False
        )
        df = pl.DataFrame(table)
        filtered_table = df.filter(*self.predicates, **self.constraints).to_arrow()

        return TableStream(
            filtered_table,
            tag_columns=stream.tag_keys(),
            source=self,
            upstreams=(stream,),
        )

    def op_validate_inputs(self, stream: cp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """

        # Any valid stream would work
        return

    def op_output_types(
        self, stream: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        # data types are not modified
        return stream.types(include_system_tags=include_system_tags)

    def op_identity_structure(self, stream: cp.Stream | None = None) -> Any:
        return (
            self.__class__.__name__,
            self.predicates,
            self.constraints,
        ) + ((stream,) if stream is not None else ())


class SelectPacketColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def op_forward(self, stream: cp.Stream) -> cp.Stream:
        tag_columns, packet_columns = stream.keys()
        packet_columns_to_drop = [c for c in packet_columns if c not in self.columns]
        new_packet_columns = [
            c for c in packet_columns if c not in packet_columns_to_drop
        ]

        if len(new_packet_columns) == len(packet_columns):
            logger.info("All packet columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            include_source=True, include_system_tags=True, sort_by_tags=False
        )
        # make sure to drop associated source fields
        associated_source_fields = [
            f"{constants.SOURCE_PREFIX}{c}" for c in packet_columns_to_drop
        ]
        packet_columns_to_drop.extend(associated_source_fields)

        modified_table = table.drop_columns(packet_columns_to_drop)

        return TableStream(
            modified_table,
            tag_columns=tag_columns,
            source=self,
            upstreams=(stream,),
        )

    def op_validate_inputs(self, stream: cp.Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        tag_columns, packet_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(packet_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing packet columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

    def op_output_types(
        self, stream: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        tag_schema, packet_schema = stream.types(
            include_system_tags=include_system_tags
        )
        _, packet_columns = stream.keys()
        packets_to_drop = [pc for pc in packet_columns if pc not in self.columns]

        # this ensures all system tag columns are preserved
        new_packet_schema = {
            k: v for k, v in packet_schema.items() if k not in packets_to_drop
        }

        return tag_schema, new_packet_schema

    def op_identity_structure(self, stream: cp.Stream | None = None) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        ) + ((stream,) if stream is not None else ())
