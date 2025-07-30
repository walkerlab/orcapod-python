from collections.abc import Collection, Iterator
from datetime import datetime
from orcapod.data.kernels import KernelStream, WrappedKernel, TrackedKernelBase
from orcapod.data.pods import ArrowDataStore, CachedPod
from orcapod.protocols import data_protocols as dp
from orcapod.types import TypeSpec
from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING, Any
from orcapod.data.system_constants import orcapod_constants as constants
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    import pandas as pd
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")


class Node(
    TrackedKernelBase,
):
    """
    Mixin class for pipeline nodes
    """

    def __init__(
        self,
        input_streams: Collection[dp.Stream],
        pipeline_store: ArrowDataStore,
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._cached_stream: KernelStream | None = None
        self.input_streams = tuple(input_streams)
        self.pipeline_store = pipeline_store
        self.pipeline_path_prefix = pipeline_path_prefix
        # compute invocation hash - note that empty () is passed into identity_structure to signify
        # identity structure of invocation with no input streams
        self.invocation_hash = self.data_context.object_hasher.hash_to_hex(
            self.identity_structure(()), prefix_hasher_id=True
        )

    @property
    def contained_kernel(self) -> dp.Kernel:
        raise NotImplementedError(
            "This property should be implemented by subclasses to return the contained kernel."
        )

    @property
    def tag_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the tag in the pipeline run records.
        This is used to store the run-associated tag info.
        """
        tag_keys, _ = self.keys()
        return tag_keys

    @property
    def packet_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the packet in the pipeline run records.
        This is used to store the run-associated packet info.
        """
        # TODO: consider caching this
        _, packet_keys = self.keys()
        return packet_keys

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        """
        Return the path to the pipeline run records.
        This is used to store the run-associated tag info.
        """
        return self.pipeline_path_prefix + self.kernel_id + (self.invocation_hash,)

    def validate_inputs(self, *processed_streams: dp.Stream) -> None:
        pass

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        if len(streams) > 0:
            raise NotImplementedError(
                "At this moment, Node does not yet support handling additional input streams."
            )
        # TODO: re-evaluate the use here
        # super().validate_inputs(*self.input_streams)
        return super().forward(*self.input_streams)

    def __call__(self, *args, **kwargs) -> KernelStream:
        if self._cached_stream is None:
            self._cached_stream = super().__call__(*args, **kwargs)
        return self._cached_stream

    # properties and methods to act as a dp.Stream
    @property
    def source(self) -> dp.Kernel | None:
        return self

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        tag_types, packet_types = self.types()
        return tuple(tag_types.keys()), tuple(packet_types.keys())

    def types(self) -> tuple[TypeSpec, TypeSpec]:
        return self.contained_kernel.output_types(*self.input_streams)

    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        Return the output types of the node.
        This is used to determine the types of the output streams.
        """
        return self.contained_kernel.output_types(*self.input_streams)

    @property
    def last_modified(self) -> datetime | None:
        return self().last_modified

    @property
    def is_current(self) -> bool:
        return self().is_current

    def __iter__(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        return self().__iter__()

    def iter_packets(self) -> Iterator[tuple[dp.Tag, dp.Packet]]:
        return self().iter_packets()

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_content_hash: bool | str = False,
    ) -> "pa.Table":
        return self().as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_content_hash=include_content_hash,
        )

    def flow(self) -> Collection[tuple[dp.Tag, dp.Packet]]:
        return self().flow()

    def identity_structure(self, streams: Collection[dp.Stream] | None = None) -> Any:
        """
        Return the identity structure of the node.
        This is used to compute the invocation hash.
        """
        # construct identity structure from the node's information and the
        # contained kernel
        if streams is not None and len(streams) > 0:
            raise NotImplementedError(
                "At this moment, Node does not yet support handling additional input streams."
            )
        return self.contained_kernel.identity_structure(self.input_streams)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all records associated with the node.
        If include_system_columns is True, system columns will be included in the result.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

    @property
    def lazy(self) -> "pl.LazyFrame | None":
        records = self.get_all_records(include_system_columns=False)
        if records is not None:
            return pl.LazyFrame(records)
        return None

    @property
    def df(self) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        lazy_df = self.lazy
        if lazy_df is not None:
            return lazy_df.collect()
        return None

    @property
    def polars_df(self) -> "pl.DataFrame | None":
        """
        Return the DataFrame representation of the pod's records.
        """
        return self.df

    @property
    def pandas_df(self) -> "pd.DataFrame | None":
        """
        Return the pandas DataFrame representation of the pod's records.
        """
        records = self.get_all_records(include_system_columns=False)
        if records is not None:
            return records.to_pandas()
        return None


class KernelNode(Node, WrappedKernel):
    """
    A node in the pipeline that represents a kernel.
    This node can be used to execute the kernel and process data streams.
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        kernel: dp.Kernel,
        input_streams: Collection[dp.Stream],
        pipeline_store: ArrowDataStore,
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            kernel=kernel,
            input_streams=input_streams,
            pipeline_store=pipeline_store,
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

    def identity_structure(self, streams: Collection[dp.Stream] | None = None) -> Any:
        """
        Return the identity structure of the node.
        This is used to compute the invocation hash.
        """
        # construct identity structure from the node's information and the
        # contained kernel
        if streams is not None and len(streams) > 0:
            raise NotImplementedError(
                "At this moment, Node does not yet support handling additional input streams."
            )
        return self.kernel.identity_structure(self.input_streams)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        output_stream = super().forward(*streams)

        self.record_pipeline_output(output_stream)
        return output_stream

    def record_pipeline_output(self, output_stream: dp.Stream) -> None:
        key_column_name = self.HASH_COLUMN_NAME
        output_table = output_stream.as_table(
            include_data_context=True,
            include_source=True,
            include_content_hash=key_column_name,
        )
        self.pipeline_store.add_records(
            self.pipeline_path,
            output_table,
            record_id_column=key_column_name,
            skip_duplicates=True,
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.pipeline_store.get_all_records(self.pipeline_path)

        if results is None:
            return None

        if not include_system_columns:
            system_columns = [
                c
                for c in results.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.CONTEXT_KEY)
                or c.startswith(constants.SOURCE_PREFIX)
            ]
            results = results.drop(system_columns)

        return results


def add_pipeline_record(
    self, tag: dp.Tag, input_packet: dp.Packet, retrieved: bool | None = None
) -> None:
    # combine dp.Tag with packet content hash to compute entry hash
    tag_with_hash = tag.as_table().append_column(
        self.PACKET_HASH_COLUMN,
        pa.array([input_packet.content_hash()], type=pa.large_string()),
    )
    entry_id = self.data_context.arrow_hasher.hash_table(
        tag_with_hash, prefix_hasher_id=True
    )

    existing_record = self.pipeline_store.get_record_by_id(
        self.pipeline_path,
        entry_id,
    )

    if existing_record is not None:
        # if the record already exists, return it
        return

    # no record matching, so construct the full record

    input_packet_info = (
        input_packet.as_table(
            include_source=True,
        )
        .append_column(
            f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}",
            pa.array([input_packet.data_context_key], type=pa.large_string()),
        )
        .append_column(
            self.DATA_RETRIEVED_FLAG,
            pa.array([retrieved], type=pa.bool_()),
        )
        .drop(input_packet.keys())
    )

    combined_record = arrow_utils.hstack_tables(tag_with_hash, input_packet_info)

    self.pipeline_store.add_record(
        self.pipeline_path,
        entry_id,
        combined_record,
        skip_duplicates=False,
    )


class PodNode(Node, CachedPod):
    def __init__(
        self,
        pod: dp.Pod,
        input_streams: Collection[dp.Stream],
        pipeline_store: ArrowDataStore,
        result_store: ArrowDataStore | None = None,
        record_path_prefix: tuple[str, ...] = (),
        pipeline_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(
            pod=pod,
            result_store=result_store,
            record_path_prefix=record_path_prefix,
            input_streams=input_streams,
            pipeline_store=pipeline_store,
            pipeline_path_prefix=pipeline_path_prefix,
            **kwargs,
        )
        self.pipeline_store = pipeline_store

    @property
    def contained_kernel(self) -> dp.Kernel:
        return self.pod

    def __repr__(self):
        return f"PodNode(pod={self.pod!r})"

    def __str__(self):
        return f"PodNode:{self.pod!s}"

    def call(
        self,
        tag: dp.Tag,
        packet: dp.Packet,
        skip_record_check: bool = False,
        skip_recording: bool = False,
    ) -> tuple[dp.Tag, dp.Packet | None]:
        tag, output_packet = super().call(
            tag,
            packet,
            skip_record_check=skip_record_check,
            skip_recording=skip_recording,
        )
        if output_packet is not None:
            retrieved = (
                output_packet.get_meta_value(self.DATA_RETRIEVED_FLAG) is not None
            )
            # add pipeline record if the output packet is not None
            self.add_pipeline_record(tag, packet, retrieved=retrieved)
        return tag, output_packet

    def add_pipeline_record(
        self, tag: dp.Tag, input_packet: dp.Packet, retrieved: bool | None = None
    ) -> None:
        # combine dp.Tag with packet content hash to compute entry hash
        tag_with_hash = tag.as_table().append_column(
            self.PACKET_HASH_COLUMN,
            pa.array([input_packet.content_hash()], type=pa.large_string()),
        )
        entry_id = self.data_context.arrow_hasher.hash_table(
            tag_with_hash, prefix_hasher_id=True
        )

        existing_record = self.pipeline_store.get_record_by_id(
            self.pipeline_path,
            entry_id,
        )

        if existing_record is not None:
            # if the record already exists, return it
            return

        # no record matching, so construct the full record

        input_packet_info = (
            input_packet.as_table(
                include_source=True,
            )
            .append_column(
                f"{constants.META_PREFIX}input_packet{constants.CONTEXT_KEY}",
                pa.array([input_packet.data_context_key], type=pa.large_string()),
            )
            .append_column(
                self.DATA_RETRIEVED_FLAG,
                pa.array([retrieved], type=pa.bool_()),
            )
            .drop(input_packet.keys())
        )

        combined_record = arrow_utils.hstack_tables(tag_with_hash, input_packet_info)

        self.pipeline_store.add_record(
            self.pipeline_path,
            entry_id,
            combined_record,
            skip_duplicates=False,
        )

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        results = self.result_store.get_all_records(
            self.record_path, record_id_column=self.PACKET_HASH_COLUMN
        )

        if self.pipeline_store is None:
            raise ValueError(
                "Pipeline store is not configured, cannot retrieve tag info"
            )
        taginfo = self.pipeline_store.get_all_records(
            self.pipeline_path,
        )

        if results is None or taginfo is None:
            return None

        # TODO: do not hardcode the join keys
        joined_info = taginfo.join(
            results,
            self.PACKET_HASH_COLUMN,
            join_type="inner",
        )

        if not include_system_columns:
            system_columns = [
                c
                for c in joined_info.column_names
                if c.startswith(constants.META_PREFIX)
                or c.startswith(constants.CONTEXT_KEY)
                or c.startswith(constants.SOURCE_PREFIX)
            ]
            joined_info = joined_info.drop(system_columns)
        return joined_info
