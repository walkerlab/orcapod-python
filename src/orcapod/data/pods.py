import logging
import sys
from abc import abstractmethod
from collections.abc import Callable, Collection, Iterable, Sequence
from typing import Any, Literal, cast

from orcapod.data.datagrams import PythonDictPacket
from orcapod.data.kernels import TrackedKernelBase
from orcapod.data.operators import Join
from orcapod.data.streams import PodStream
from orcapod.hashing.hash_utils import get_function_signature
from orcapod.protocols import data_protocols as dp
from orcapod.protocols import hashing_protocols as hp
from orcapod.types import SemanticTypeRegistry, TypeSpec, default_registry
from orcapod.types import typespec_utils as tsutils

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
        input_streams = self.pre_processing_step(*streams)
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

    def pre_processing_step(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Prepare the incoming streams for execution in the pod. This default implementation
        joins all the input streams together.
        """
        # if multiple streams are provided, join them
        # otherwise, return as is
        combined_streams = list(streams)
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join()(stream, next_stream)
            combined_streams = [stream]

        return tuple(combined_streams)

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
        semantic_type_registry: SemanticTypeRegistry | None = None,
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

        if semantic_type_registry is None:
            # TODO: reconsider the use of default registry here
            semantic_type_registry = default_registry

        self.semantic_type_registry = semantic_type_registry
        self.function_info_extractor = function_info_extractor

        # extract input and output types from the function signature
        self._input_packet_types, self._output_packet_types = (
            tsutils.extract_function_typespecs(
                self.function,
                self.output_keys,
                input_typespec=input_typespec,
                output_typespec=output_typespec,
            )
        )

    def input_packet_types(self) -> TypeSpec:
        """
        Return the input typespec for the function pod.
        This is used to validate the input streams.
        """
        return self._input_packet_types

    def output_packet_types(self) -> TypeSpec:
        """
        Return the output typespec for the function pod.
        This is used to validate the output streams.
        """
        return self._output_packet_types

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

    def call(
        self, tag: dp.Tag, packet: dp.Packet
    ) -> tuple[dp.Tag, PythonDictPacket | None]:
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

        # TODO: add source info based on this function call
        output_packet = PythonDictPacket(
            {k: v for k, v in zip(self.output_keys, output_values)}
        )
        return tag, output_packet

    def identity_structure(self, *streams: dp.Stream) -> Any:
        # construct identity structure for the function

        # if function_info_extractor is available, use that but substitute the function_name
        if self.function_info_extractor is not None:
            function_info = self.function_info_extractor.extract_function_info(
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
        function_info["output_keys"] = tuple(self.output_keys)

        id_struct = (
            self.__class__.__name__,
            function_info,
        )
        # if streams are provided, perform pre-processing step, validate, and add the
        # resulting single stream to the identity structure
        if len(streams) > 0:
            processed_streams = self.pre_processing_step(*streams)
            self.validate_inputs(*processed_streams)
            id_struct += (processed_streams[0],)

        return id_struct


class WrappedPod(ActivatablePodBase):
    """
    A wrapper for a pod that allows it to be used as a kernel.
    This class is meant to serve as a base class for other pods that need to wrap existing pods.
    """

    def __init__(
        self,
        pod: dp.Pod,
        fixed_input_streams: tuple[dp.Stream, ...] | None = None,
        label: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(fixed_input_streams=fixed_input_streams, label=label, **kwargs)
        self.pod = pod

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

    def __init__(self, pod: dp.Pod, cache_key: str, **kwargs):
        super().__init__(pod, **kwargs)
        self.cache_key = cache_key
        self.cache: dict[str, dp.Packet] = {}
