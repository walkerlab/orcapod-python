from abc import abstractmethod
from einops import pack
from orcapod.core.kernels import KernelStream, WrappedKernel
from orcapod.core.sources import SourceBase
from orcapod.core.pods import CachedPod
from orcapod.protocols import core_protocols as dp, database_protocols as dbp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING, Any
from orcapod.core.system_constants import constants
from orcapod.utils import arrow_utils
from collections.abc import Collection

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    import pandas as pd
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")


class NodeBase(
    SourceBase,
):
    """
    Mixin class for pipeline nodes
    """

    def __init__(
        self,
        input_streams: Collection[dp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._cached_stream: KernelStream | None = None
        self.input_streams = tuple(input_streams)
        self.pipeline_path_prefix = pipeline_path_prefix
        # compute invocation hash - note that empty () is passed into identity_structure to signify
        # identity structure of invocation with no input streams
        self.pipeline_node_hash = self.data_context.object_hasher.hash_object(
            self.identity_structure(())
        ).to_string()
        tag_types, packet_types = self.types(include_system_tags=True)
        self.tag_schema_hash = self.data_context.object_hasher.hash_object(
            tag_types
        ).to_string()
        self.packet_schema_hash = self.data_context.object_hasher.hash_object(
            packet_types
        ).to_string()
        self.pipeline_database = pipeline_database

    @property
    def contained_kernel(self) -> dp.Kernel:
        raise NotImplementedError(
            "This property should be implemented by subclasses to return the contained kernel."
        )

    @property
    def reference(self) -> tuple[str, ...]:
        return self.contained_kernel.reference

    @property
    @abstractmethod
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        ...

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        if len(streams) > 0:
            raise NotImplementedError(
                "At this moment, Node does not yet support handling additional input streams."
            )
        # TODO: re-evaluate the use here
        # super().validate_inputs(*self.input_streams)
        return super().forward(*self.input_streams)  # type: ignore[return-value]

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Return the output types of the node.
        This is used to determine the types of the output streams.
        """
        return self.contained_kernel.output_types(
            *self.input_streams, include_system_tags=include_system_tags
        )

    def source_identity_structure(self) -> Any:
        """
        Return the identity structure of the node.
        This is used to compute the invocation hash.
        """
        # construct identity structure from the node's information and the
        return self.contained_kernel.identity_structure(self.input_streams)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all records associated with the node.
        If include_system_columns is True, system columns will be included in the result.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")


class KernelNode(NodeBase, WrappedKernel):
    """
    A node in the pipeline that represents a kernel.
    This node can be used to execute the kernel and process data streams.
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        kernel: dp.Kernel,
        input_streams: Collection[dp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            kernel=kernel,
            input_streams=input_streams,
            pipeline_database=pipeline_database,
            pipeline_path_prefix=pipeline_path_prefix,
            **kwargs,
        )

    @property
    def contained_kernel(self) -> dp.Kernel:
        return self.kernel

    def __repr__(self):
        return f"KernelNode(kernel={self.kernel!r})"

    def __str__(self):
        return f"KernelNode:{self.kernel!s}"

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        output_stream = super().forward(*streams)

        self.record_pipeline_output(output_stream)
        return output_stream

    def record_pipeline_output(self, output_stream: dp.Stream) -> None:
        key_column_name = self.HASH_COLUMN_NAME
        output_table = output_stream.as_table(
            include_data_context=True,
            include_system_tags=True,
            include_source=True,
            include_content_hash=key_column_name,
        )
        self.pipeline_database.add_records(
            self.pipeline_path,
            output_table,
            record_id_column=key_column_name,
            skip_duplicates=True,
        )

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        return (
            self.pipeline_path_prefix  # pipeline ID
            + self.reference  # node ID
            + (
                self.pipeline_node_hash,  # pipeline node ID
                self.packet_schema_hash,  # packet schema ID
                self.tag_schema_hash,  # tag schema ID
            )
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.pipeline_database.get_all_records(self.pipeline_path)

        if results is None:
            return None

        if not include_system_columns:
            system_columns = [
                c
                for c in results.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.DATAGRAM_PREFIX)
            ]
            results = results.drop(system_columns)

        return results


class PodNode(NodeBase, CachedPod):
    def __init__(
        self,
        pod: dp.Pod,
        input_streams: Collection[dp.Stream],
        pipeline_database: dbp.ArrowDatabase,
        result_database: dbp.ArrowDatabase | None = None,
        record_path_prefix: tuple[str, ...] = (),
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            pod=pod,
            result_database=result_database,
            record_path_prefix=record_path_prefix,
            input_streams=input_streams,
            pipeline_database=pipeline_database,
            pipeline_path_prefix=pipeline_path_prefix,
            **kwargs,
        )

    @property
    def contained_kernel(self) -> dp.Kernel:
        return self.pod

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        return (
            self.pipeline_path_prefix  # pipeline ID
            + self.reference  # node ID
            + (
                self.pipeline_node_hash,  # pipeline node ID
                self.tag_schema_hash,  # tag schema ID
            )
        )

    def __repr__(self):
        return f"PodNode(pod={self.pod!r})"

    def __str__(self):
        return f"PodNode:{self.pod!s}"

    def call(
        self,
        tag: dp.Tag,
        packet: dp.Packet,
        record_id: str | None = None,
        execution_engine: dp.ExecutionEngine | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[dp.Tag, dp.Packet | None]:
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        if record_id is None:
            record_id = self.get_record_id(packet, execution_engine_hash)

        tag, output_packet = super().call(
            tag,
            packet,
            record_id=record_id,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
            execution_engine=execution_engine,
        )

        if output_packet is not None:
            retrieved = (
                output_packet.get_meta_value(self.DATA_RETRIEVED_FLAG) is not None
            )
            # add pipeline record if the output packet is not None
            # TODO: verify cache lookup logic
            self.add_pipeline_record(
                tag,
                packet,
                record_id,
                retrieved=retrieved,
                skip_cache_lookup=skip_cache_lookup,
            )
        return tag, output_packet

    async def async_call(
        self,
        tag: dp.Tag,
        packet: dp.Packet,
        record_id: str | None = None,
        execution_engine: dp.ExecutionEngine | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[dp.Tag, dp.Packet | None]:
        execution_engine_hash = execution_engine.name if execution_engine else "default"
        if record_id is None:
            record_id = self.get_record_id(packet, execution_engine_hash)

        tag, output_packet = await super().async_call(
            tag,
            packet,
            record_id=record_id,
            skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
            execution_engine=execution_engine,
        )

        if output_packet is not None:
            retrieved = (
                output_packet.get_meta_value(self.DATA_RETRIEVED_FLAG) is not None
            )
            # add pipeline record if the output packet is not None
            # TODO: verify cache lookup logic
            self.add_pipeline_record(
                tag,
                packet,
                record_id,
                retrieved=retrieved,
                skip_cache_lookup=skip_cache_lookup,
            )
        return tag, output_packet

    def add_pipeline_record(
        self,
        tag: dp.Tag,
        input_packet: dp.Packet,
        packet_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None:
        # combine dp.Tag with packet content hash to compute entry hash
        # TODO: add system tag columns
        # TODO: consider using bytes instead of string representation
        tag_with_hash = tag.as_table(include_system_tags=True).append_column(
            constants.INPUT_PACKET_HASH,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
        )

        # unique entry ID is determined by the combination of tags, system_tags, and input_packet hash
        entry_id = self.data_context.arrow_hasher.hash_table(tag_with_hash).to_string()

        # check presence of an existing entry with the same entry_id
        existing_record = None
        if not skip_cache_lookup:
            existing_record = self.pipeline_database.get_record_by_id(
                self.pipeline_path,
                entry_id,
            )

        if existing_record is not None:
            # if the record already exists, then skip
            return

        # rename all keys to avoid potential collision with result columns
        renamed_input_packet = input_packet.rename(
            {k: f"_input_{k}" for k in input_packet.keys()}
        )
        input_packet_info = (
            renamed_input_packet.as_table(include_source=True)
            .append_column(
                constants.PACKET_RECORD_ID,
                pa.array([packet_record_id], type=pa.large_string()),
            )
            .append_column(
                f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}",
                pa.array([input_packet.data_context_key], type=pa.large_string()),
            )
            .append_column(
                self.DATA_RETRIEVED_FLAG,
                pa.array([retrieved], type=pa.bool_()),
            )
            .drop_columns(list(renamed_input_packet.keys()))
        )

        combined_record = arrow_utils.hstack_tables(
            tag.as_table(include_system_tags=True), input_packet_info
        )

        self.pipeline_database.add_record(
            self.pipeline_path,
            entry_id,
            combined_record,
            skip_duplicates=False,
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.result_database.get_all_records(
            self.record_path, record_id_column=constants.PACKET_RECORD_ID
        )

        if self.pipeline_database is None:
            raise ValueError(
                "Pipeline database is not configured, cannot retrieve tag info"
            )
        taginfo = self.pipeline_database.get_all_records(
            self.pipeline_path,
        )

        if results is None or taginfo is None:
            return None

        # hack - use polars for join as it can deal with complex data type
        # TODO: convert the entire load logic to use polars with lazy evaluation

        joined_info = (
            pl.DataFrame(taginfo)
            .join(pl.DataFrame(results), on=constants.PACKET_RECORD_ID, how="inner")
            .to_arrow()
        )

        # joined_info = taginfo.join(
        #     results,
        #     constants.PACKET_RECORD_ID,
        #     join_type="inner",
        # )

        if not include_system_columns:
            system_columns = [
                c
                for c in joined_info.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.DATAGRAM_PREFIX)
            ]
            joined_info = joined_info.drop(system_columns)
        return joined_info
