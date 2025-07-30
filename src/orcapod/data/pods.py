import logging
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from orcapod.data.context import DataContext
from orcapod.data.datagrams import (
    ArrowPacket,
    DictPacket,
)
from orcapod.data.kernels import KernelStream, TrackedKernelBase
from orcapod.data.operators import Join
from orcapod.data.streams import LazyPodResultStream
from orcapod.data.system_constants import orcapod_constants as constants
from orcapod.hashing.hash_utils import get_function_signature
from orcapod.protocols import data_protocols as dp
from orcapod.protocols import hashing_protocols as hp
from orcapod.protocols.store_protocols import ArrowDataStore
from orcapod.types import TypeSpec
from orcapod.types import typespec_utils as tsutils
from orcapod.types.schemas import PythonSchema
from orcapod.types.semantic_converter import SemanticConverter
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

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

    def __init__(
        self,
        error_handling: error_handling_options = "raise",
        label: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self._active = True
        self.error_handling = error_handling

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        Return the input and output typespecs for the pod.
        This is used to validate the input and output streams.
        """
        tag_typespec, _ = streams[0].types()
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

    def pre_kernel_processing(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Prepare the incoming streams for execution in the pod. At least one stream must be present.
        If more than one stream is present, the join of the provided streams will be returned.
        """
        # if multiple streams are provided, join them
        # otherwise, return as is
        if len(streams) <= 1:
            return streams

        output_stream = self._join_streams(*streams)
        return (output_stream,)

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

    def prepare_output_stream(
        self, *streams: dp.Stream, label: str | None = None
    ) -> KernelStream:
        return KernelStream(source=self, upstreams=streams, label=label)

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        assert len(streams) == 1, "PodBase.forward expects exactly one input stream"
        return LazyPodResultStream(pod=self, prepared_stream=streams[0])

    @abstractmethod
    def call(
        self, tag: dp.Tag, packet: dp.Packet
    ) -> tuple[dp.Tag, dp.Packet | None]: ...

    def track_invocation(self, *streams: dp.Stream, label: str | None = None) -> None:
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_pod_invocation(self, streams, label=label)


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

        # now compute hash for the self and store that info
        self._pod_hash = self.data_context.object_hasher.hash_to_hex(
            self, prefix_hasher_id=True
        )

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (self.function_name, self._pod_hash)

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
        source_info = {
            k: ":".join(self.kernel_id + (packet.content_hash(), k))
            for k in output_data
        }

        output_packet = DictPacket(
            {k: v for k, v in zip(self.output_keys, output_values)},
            source_info=source_info,
            typespec=self.output_packet_types(),
            semantic_converter=self._output_semantic_converter,
            data_context=self._data_context,
        )
        return tag, output_packet

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
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
        if streams is not None and len(streams) != 0:
            id_struct += tuple(streams)

        return id_struct


class WrappedPod(ActivatablePodBase):
    """
    A wrapper for an existing pod, allowing for additional functionality or modifications without changing the original pod.
    This class is meant to serve as a base class for other pods that need to wrap existing pods.
    Note that only the call logic is pass through to the wrapped pod, but the forward logic is not.
    """

    def __init__(
        self,
        pod: dp.Pod,
        label: str | None = None,
        data_context: str | DataContext | None = None,
        **kwargs,
    ) -> None:
        if data_context is None:
            data_context = pod.data_context_key
        super().__init__(
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

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.pod.validate_inputs(*streams)

    def call(self, tag: dp.Tag, packet: dp.Packet) -> tuple[dp.Tag, dp.Packet | None]:
        output_tag, output_packet = self.pod.call(tag, packet)
        return output_tag, output_packet

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        return self.pod.identity_structure(streams)

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
    DATA_RETRIEVED_FLAG = f"{constants.META_PREFIX}data_retrieved"

    def __init__(
        self,
        pod: dp.Pod,
        result_store: ArrowDataStore,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ):
        super().__init__(pod, **kwargs)
        self.record_path_prefix = record_path_prefix
        self.result_store = result_store
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
        skip_record_check: bool = False,
        skip_recording: bool = False,
    ) -> tuple[dp.Tag, dp.Packet | None]:
        # TODO: consider logic for overwriting existing records
        output_packet = None
        if not skip_record_check:
            output_packet = self.get_recorded_output_packet(packet)
        if output_packet is None:
            tag, output_packet = super().call(tag, packet)
            if output_packet is not None and not skip_recording:
                self.record_packet(packet, output_packet)

        return tag, output_packet

    def record_packet(
        self,
        input_packet: dp.Packet,
        output_packet: dp.Packet,
        skip_duplicates: bool = False,
    ) -> dp.Packet:
        """
        Record the output packet against the input packet in the result store.
        """
        data_table = output_packet.as_table(include_context=True, include_source=True)

        result_flag = self.result_store.add_record(
            self.record_path,
            input_packet.content_hash(),
            data_table,
            skip_duplicates=skip_duplicates,
        )
        # if result_flag is None:
        #     # TODO: do more specific error handling
        #     raise ValueError(
        #         f"Failed to record packet {input_packet} in result store {self.result_store}"
        #     )
        # # TODO: make store return retrieved table
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

        # note that data context will be loaded from the result store
        return ArrowPacket(
            result_table,
            meta_info={self.DATA_RETRIEVED_FLAG: str(datetime.now(timezone.utc))},
        )
