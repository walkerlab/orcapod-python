import logging
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from typing import Any, Literal, cast

from orcapod.data.datagrams import (
    DictPacket,
    ArrowPacket,
)
from orcapod.data.context import DataContext
from orcapod.data.kernels import TrackedKernelBase
from orcapod.data.operators import Join
from orcapod.data.streams import PodStream
from orcapod.hashing.hash_utils import get_function_signature
from orcapod.protocols import data_protocols as dp
from orcapod.protocols import hashing_protocols as hp
from orcapod.protocols.store_protocols import ArrowDataStore
from orcapod.types import TypeSpec
from orcapod.types.schemas import PythonSchema
from orcapod.types.semantic_converter import SemanticConverter
from orcapod.types import typespec_utils as tsutils
from orcapod.utils import arrow_utils
from orcapod.data.system_constants import orcapod_constants as constants
import pyarrow as pa

logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]


class ActivatablePodBase(TrackedKernelBase):
    """
    FunctionPod is a specialized kernel that encapsulates a function to be executed on data streams.
    It allows for the execution of a function with a specific label and can be tracked by the system.
    """

    @abstractmethod
    def input_packet_types(self) -> TypeSpec:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        ...

    @abstractmethod
    def output_packet_types(self) -> TypeSpec:
        """
        Return the output typespec for the pod. This is used to validate the output streams.
        """
        ...

    @abstractmethod
    def call(
        self, tag: dp.Tag, packet: dp.Packet
    ) -> tuple[dp.Tag, dp.Packet | None]: ...

    def __init__(
        self,
        fixed_input_streams: tuple[dp.Stream, ...] | None = None,
        error_handling: error_handling_options = "raise",
        label: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(fixed_input_streams=fixed_input_streams, label=label, **kwargs)
        self._active = True
        self.error_handling = error_handling

    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        Return the input and output typespecs for the pod.
        This is used to validate the input and output streams.
        """
        input_streams = self.pre_process_input_streams(*streams)
        self.validate_inputs(*input_streams)
        tag_typespec, _ = input_streams[0].types()
        return tag_typespec, self.output_packet_types()

    def is_active(self) -> bool:
        """
        Check if the pod is active. If not, it will not process any packets.
        """
        return self._active

    def set_active(self, active: bool) -> None:
        """
        Set the active state of the pod. If set to False, the pod will not process any packets.
        """
        self._active = active

    def validate_inputs(self, *streams: dp.Stream) -> None:
        if len(streams) != 1:
            raise ValueError(
                f"{self.__class__.__name__} expects exactly one input stream, got {len(streams)}"
            )
        input_stream = streams[0]
        _, incoming_packet_types = input_stream.types()
        if not tsutils.check_typespec_compatibility(
            incoming_packet_types, self.input_packet_types()
        ):
            # TODO: use custom exception type for better error handling
            raise ValueError(
                f"Input typespec {incoming_packet_types} is not compatible with expected input typespec {self.input_packet_types}"
            )

    @staticmethod
    def _join_streams(*streams: dp.Stream) -> dp.Stream:
        if not streams:
            raise ValueError("No streams provided for joining")
        # Join the streams using a suitable join strategy
        if len(streams) == 1:
            return streams[0]

        joined_stream = streams[0]
        for next_stream in streams[1:]:
            joined_stream = Join()(joined_stream, next_stream)
        return joined_stream

    def pre_process_input_streams(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Prepare the incoming streams for execution in the pod. If fixed_input_streams are present,
        they will be used as the input streams and the newly provided streams would be used to
        restrict (semijoin) the fixed streams.
        Otherwise, the join of the provided streams will be returned.
        """
        # if multiple streams are provided, join them
        # otherwise, return as is
        if self.fixed_input_streams is not None and len(streams) > 0:
            output_stream = self._join_streams(*self.fixed_input_streams)
            if len(streams) > 0:
                restrict_stream = self._join_streams(*streams)
                # output_stream = SemiJoin()(output_stream, restrict_stream)
        else:
            if len(streams) == 0:
                raise ValueError(
                    f"{self.__class__.__name__} expects at least one input stream"
                )
            output_stream = self._join_streams(*streams)
        return (output_stream,)

    def prepare_output_stream(
        self, *streams: dp.Stream, label: str | None = None
    ) -> dp.LiveStream:
        output_stream = self.forward(*streams)
        output_stream.label = label
        return output_stream

    def track_invocation(self, *streams: dp.Stream) -> None:
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_pod_invocation(self, streams)

    def forward(self, *streams: dp.Stream) -> PodStream:
        assert len(streams) == 1, "PodBase.forward expects exactly one input stream"
        input_stream = streams[0]

        return PodStream(
            self,
            input_stream,
            error_handling=cast(error_handling_options, self.error_handling),
        )


def function_pod(
    output_keys: str | Collection[str] | None = None,
    function_name: str | None = None,
    label: str | None = None,
    **kwargs,
) -> Callable[..., "FunctionPod"]:
    """
    Decorator that wraps a function in a FunctionPod instance.

    Args:
        output_keys: Keys for the function output(s)
        function_name: Name of the function pod; if None, defaults to the function name
        **kwargs: Additional keyword arguments to pass to the FunctionPod constructor. Please refer to the FunctionPod documentation for details.

    Returns:
        FunctionPod instance wrapping the decorated function
    """

    def decorator(func) -> FunctionPod:
        if func.__name__ == "<lambda>":
            raise ValueError("Lambda functions cannot be used with function_pod")

        if not hasattr(func, "__module__") or func.__module__ is None:
            raise ValueError(
                f"Function {func.__name__} must be defined at module level"
            )

        # Store the original function in the module for pickling purposes
        # and make sure to change the name of the function
        module = sys.modules[func.__module__]
        base_function_name = func.__name__
        new_function_name = f"_original_{func.__name__}"
        setattr(module, new_function_name, func)
        # rename the function to be consistent and make it pickleable
        setattr(func, "__name__", new_function_name)
        setattr(func, "__qualname__", new_function_name)

        # Create a simple typed function pod
        pod = FunctionPod(
            function=func,
            output_keys=output_keys,
            function_name=function_name or base_function_name,
            label=label,
            **kwargs,
        )
        return pod

    return decorator


class FunctionPod(ActivatablePodBase):
    def __init__(
        self,
        function: dp.PodFunction,
        output_keys: str | Collection[str] | None = None,
        function_name=None,
        input_typespec: TypeSpec | None = None,
        output_typespec: TypeSpec | Sequence[type] | None = None,
        label: str | None = None,
        function_info_extractor: hp.FunctionInfoExtractor | None = None,
        **kwargs,
    ) -> None:
        self.function = function

        if output_keys is None:
            output_keys = []
        if isinstance(output_keys, str):
            output_keys = [output_keys]
        self.output_keys = output_keys
        if function_name is None:
            if hasattr(self.function, "__name__"):
                function_name = getattr(self.function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__ attribute"
                )
        self.function_name = function_name
        super().__init__(label=label or self.function_name, **kwargs)

        # extract input and output types from the function signature
        input_packet_types, output_packet_types = tsutils.extract_function_typespecs(
            self.function,
            self.output_keys,
            input_typespec=input_typespec,
            output_typespec=output_typespec,
        )
        self._input_packet_schema = PythonSchema(input_packet_types)
        self._output_packet_schema = PythonSchema(output_packet_types)
        self._output_semantic_converter = SemanticConverter.from_semantic_schema(
            self._output_packet_schema.to_semantic_schema(
                semantic_type_registry=self.data_context.semantic_type_registry
            )
        )

        self._function_info_extractor = function_info_extractor

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (
            self.function_name,
            self.data_context.object_hasher.hash_to_hex(self),
        )

    def input_packet_types(self) -> PythonSchema:
        """
        Return the input typespec for the function pod.
        This is used to validate the input streams.
        """
        return self._input_packet_schema.copy()

    def output_packet_types(self) -> PythonSchema:
        """
        Return the output typespec for the function pod.
        This is used to validate the output streams.
        """
        return self._output_packet_schema.copy()

    def __repr__(self) -> str:
        return f"FunctionPod:{self.function_name}"

    def __str__(self) -> str:
        include_module = self.function.__module__ != "__main__"
        func_sig = get_function_signature(
            self.function,
            name_override=self.function_name,
            include_module=include_module,
        )
        return f"FunctionPod:{func_sig}"

    def call(self, tag: dp.Tag, packet: dp.Packet) -> tuple[dp.Tag, DictPacket | None]:
        v: dp.Packet = DictPacket({})
        print(v)
        if not self.is_active():
            logger.info(
                f"Pod is not active: skipping computation on input packet {packet}"
            )
            return tag, None
        output_values = []

        # any kernel/pod invocation happening inside the function will NOT be tracked
        with self._tracker_manager.no_tracking():
            values = self.function(**packet.as_dict(include_source=False))

        if len(self.output_keys) == 0:
            output_values = []
        elif len(self.output_keys) == 1:
            output_values = [values]  # type: ignore
        elif isinstance(values, Iterable):
            output_values = list(values)  # type: ignore
        elif len(self.output_keys) > 1:
            raise ValueError(
                "Values returned by function must be a pathlike or a sequence of pathlikes"
            )

        if len(output_values) != len(self.output_keys):
            raise ValueError(
                f"Number of output keys {len(self.output_keys)}:{self.output_keys} does not match number of values returned by function {len(output_values)}"
            )

        output_data = {k: v for k, v in zip(self.output_keys, output_values)}
        source_info = {k: ":".join(self.kernel_id + (k,)) for k in output_data}

        output_packet = DictPacket(
            {k: v for k, v in zip(self.output_keys, output_values)},
            source_info=source_info,
            typespec=self.output_packet_types(),
            semantic_converter=self._output_semantic_converter,
            data_context=self._data_context,
        )
        return tag, output_packet

    def identity_structure(self, *streams: dp.Stream) -> Any:
        # construct identity structure for the function

        # if function_info_extractor is available, use that but substitute the function_name
        if self._function_info_extractor is not None:
            function_info = self._function_info_extractor.extract_function_info(
                self.function,
                function_name=self.function_name,
                input_typespec=self.input_packet_types(),
                output_typespec=self.output_packet_types(),
            )
        else:
            # use basic information only
            function_info = {
                "name": self.function_name,
                "input_packet_types": self.input_packet_types(),
                "output_packet_types": self.output_packet_types(),
            }

        id_struct = (
            self.__class__.__name__,
            function_info,
        )
        # if streams are provided, perform pre-processing step, validate, and add the
        # resulting single stream to the identity structure
        if len(streams) > 0:
            # TODO: extract the common handling of input streams
            processed_streams = self.pre_process_input_streams(*streams)
            self.validate_inputs(*processed_streams)
            id_struct += (processed_streams[0],)

        return id_struct


class WrappedPod(ActivatablePodBase):
    """
    A wrapper for an existing pod, allowing for additional functionality or modifications without changing the original pod.
    This class is meant to serve as a base class for other pods that need to wrap existing pods.
    """

    def __init__(
        self,
        pod: dp.Pod,
        fixed_input_streams: tuple[dp.Stream, ...] | None = None,
        label: str | None = None,
        data_context: str | DataContext | None = None,
        **kwargs,
    ) -> None:
        if data_context is None:
            data_context = pod.data_context_key
        super().__init__(
            fixed_input_streams=fixed_input_streams,
            label=label,
            data_context=data_context,
            **kwargs,
        )
        self.pod = pod

    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Return the pod ID, which is the function name of the wrapped pod.
        This is used to identify the pod in the system.
        """
        return self.pod.kernel_id

    def computed_label(self) -> str | None:
        return self.pod.label

    def input_packet_types(self) -> TypeSpec:
        """
        Return the input typespec for the stored pod.
        This is used to validate the input streams.
        """
        return self.pod.input_packet_types()

    def output_packet_types(self) -> TypeSpec:
        """
        Return the output typespec for the stored pod.
        This is used to validate the output streams.
        """
        return self.pod.output_packet_types()

    def call(self, tag: dp.Tag, packet: dp.Packet) -> tuple[dp.Tag, dp.Packet | None]:
        return self.pod.call(tag, packet)

    def identity_structure(self, *streams: dp.Stream) -> Any:
        return self.pod.identity_structure(*streams)

    def __repr__(self) -> str:
        return f"WrappedPod({self.pod!r})"

    def __str__(self) -> str:
        return f"WrappedPod:{self.pod!s}"


class CachedPod(WrappedPod):
    """
    A pod that caches the results of the wrapped pod.
    This is useful for pods that are expensive to compute and can benefit from caching.
    """

    # name of the column in the tag store that contains the packet hash
    PACKET_HASH_COLUMN = f"{constants.META_PREFIX}packet_hash"

    def __init__(
        self,
        pod: dp.Pod,
        result_store: ArrowDataStore,
        pipeline_store: ArrowDataStore | None,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ):
        super().__init__(pod, **kwargs)
        self.record_path_prefix = record_path_prefix
        self.result_store = result_store
        self.pipeline_store = pipeline_store
        # unset data_context native to the object

        self.pod_hash = self.data_context.object_hasher.hash_to_hex(
            self.pod, prefix_hasher_id=True
        )

    @property
    def record_path(self) -> tuple[str, ...]:
        """
        Return the path to the record in the result store.
        This is used to store the results of the pod.
        """
        return self.record_path_prefix + self.kernel_id

    def call(
        self,
        tag: dp.Tag,
        packet: dp.Packet,
        skip_recording: bool = False,
    ) -> tuple[dp.Tag, dp.Packet | None]:
        output_packet = self.get_recorded_output_packet(packet)
        if output_packet is None:
            tag, output_packet = self.pod.call(tag, packet)
            if output_packet is not None and not skip_recording:
                self.record_packet(packet, output_packet)

        if output_packet is not None:
            self.add_pipeline_record(tag, input_packet=packet)
        return tag, output_packet

    def add_pipeline_record(self, tag: dp.Tag, input_packet: dp.Packet) -> None:
        if self.pipeline_store is None:
            # no pipeline store configured, skip recording
            return
        # combine dp.Tag with packet content hash to compute entry hash
        tag_with_hash = tag.as_table().append_column(
            self.PACKET_HASH_COLUMN,
            pa.array([input_packet.content_hash()], type=pa.large_string()),
        )
        entry_id = self.data_context.arrow_hasher.hash_table(
            tag_with_hash, prefix_hasher_id=True
        )

        existing_record = self.pipeline_store.get_record_by_id(
            self.record_path,
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
            .drop(input_packet.keys())
        )

        combined_record = arrow_utils.hstack_tables(tag_with_hash, input_packet_info)

        self.pipeline_store.add_record(
            self.record_path,
            entry_id,
            combined_record,
            ignore_duplicates=False,
        )

    def record_packet(
        self,
        input_packet: dp.Packet,
        output_packet: dp.Packet,
        ignore_duplicates: bool = False,
    ) -> dp.Packet:
        """
        Record the output packet against the input packet in the result store.
        """
        data_table = output_packet.as_table(include_context=True, include_source=True)

        result_flag = self.result_store.add_record(
            self.record_path,
            input_packet.content_hash(),
            data_table,
            ignore_duplicates=ignore_duplicates,
        )
        if result_flag is None:
            # TODO: do more specific error handling
            raise ValueError(
                f"Failed to record packet {input_packet} in result store {self.result_store}"
            )
        # TODO: make store return retrieved table
        return output_packet

    def get_recorded_output_packet(self, input_packet: dp.Packet) -> dp.Packet | None:
        """
        Retrieve the output packet from the result store based on the input packet.
        If the output packet is not found, return None.
        """
        result_table = self.result_store.get_record_by_id(
            self.record_path, input_packet.content_hash()
        )
        if result_table is None:
            return None

        return ArrowPacket(result_table)

    def _get_all_records(self) -> "pa.Table | None":
        results = self.result_store.get_all_records(
            self.record_path, record_id_column=self.PACKET_HASH_COLUMN
        )

        if self.pipeline_store is None:
            raise ValueError(
                "Pipeline store is not configured, cannot retrieve tag info"
            )
        taginfo = self.pipeline_store.get_all_records(
            self.record_path,
        )

        if results is None or taginfo is None:
            return None

        tag_columns = [
            c
            for c in taginfo.column_names
            if not c.startswith(constants.META_PREFIX)
            and not c.startswith(constants.SOURCE_PREFIX)
        ]

        packet_columns = [
            c for c in results.column_names if c != self.PACKET_HASH_COLUMN
        ]

        # TODO: do not hardcode the join keys
        joined_info = taginfo.join(
            results,
            self.PACKET_HASH_COLUMN,
            join_type="inner",
        )

        joined_info = joined_info.select([*tag_columns, *packet_columns])
        return joined_info
