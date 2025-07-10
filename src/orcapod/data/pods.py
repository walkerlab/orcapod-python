from orcapod.data.datagrams import PythonDictPacket
from orcapod.data.streams import PodStream
from orcapod.data.kernels import TrackedKernelBase
from orcapod.protocols import data_protocols as dp, hashing_protocols as hp
from orcapod.types import SemanticTypeRegistry, default_registry
from orcapod.types import typespec_utils as tsutils
from abc import abstractmethod

import logging
import sys
from collections.abc import Callable, Collection, Iterable, Sequence
from typing import Any, Literal, cast


from orcapod.types.typespec_utils import (
    extract_function_typespecs,
    check_typespec_compatibility,
)
from orcapod.types import TypeSpec

from orcapod.hashing.legacy_core import get_function_signature
from orcapod.data.operators import Join


logger = logging.getLogger(__name__)

error_handling_options = Literal["raise", "ignore", "warn"]


class PodBase(TrackedKernelBase):
    """
    FunctionPod is a specialized kernel that encapsulates a function to be executed on data streams.
    It allows for the execution of a function with a specific label and can be tracked by the system.
    """

    def types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        """
        Return the input and output typespecs for the pod.
        This is used to validate the input and output streams.
        """
        input_stream = self.process_and_verify_streams(*streams)
        tag_typespec, _ = input_stream.types()
        return tag_typespec, self.output_typespec

    @property
    @abstractmethod
    def input_typespec(self) -> TypeSpec:
        """
        Return the input typespec for the pod. This is used to validate the input streams.
        """
        ...

    @property
    @abstractmethod
    def output_typespec(self) -> TypeSpec:
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
        error_handling: error_handling_options = "raise",
        label: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self._active = True
        self.error_handling = error_handling

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

    def process_and_verify_streams(self, *streams: dp.Stream) -> dp.Stream:
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
        input_stream = combined_streams[0]
        _, input_typespec = input_stream.types()
        if not tsutils.check_typespec_compatibility(
            input_typespec, self.input_typespec
        ):
            raise ValueError(
                f"Input typespec {input_typespec} is not compatible with expected input typespec {self.input_typespec}"
            )
        return input_stream

    def validate_inputs(self, *streams: dp.Stream) -> None:
        self.process_and_verify_streams(*streams)

    def forward(self, *streams: dp.Stream) -> PodStream:
        input_stream = self.process_and_verify_streams(*streams)
        # at this point, streams should have been joined into one

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


class FunctionPod(PodBase):
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
        self._input_typespec, self._output_typespec = extract_function_typespecs(
            self.function,
            self.output_keys,
            input_typespec=input_typespec,
            output_typespec=output_typespec,
        )

    @property
    def input_typespec(self) -> TypeSpec:
        """
        Return the input typespec for the function pod.
        This is used to validate the input streams.
        """
        return self._input_typespec

    @property
    def output_typespec(self) -> TypeSpec:
        """
        Return the output typespec for the function pod.
        This is used to validate the output streams.
        """
        return self._output_typespec

    def __repr__(self) -> str:
        return f"FunctionPod:{self.function!r}"

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
                input_typespec=self.input_typespec,
                output_typespec=self.output_typespec,
            )
        else:
            # use basic information only
            function_info = {
                "name": self.function_name,
                "input_typespec": self.input_typespec,
                "output_typespec": self.output_typespec,
            }
        function_info["output_keys"] = tuple(self.output_keys)

        return (
            self.__class__.__name__,
            function_info,
        ) + streams


class StoredPod(PodBase):
    def __init__(self, pod: dp.Pod, label: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pod = pod

    def computed_label(self) -> str | None:
        return self.pod.label

    @property
    def input_typespec(self) -> TypeSpec:
        """
        Return the input typespec for the stored pod.
        This is used to validate the input streams.
        """
        return self.pod.input_typespec

    @property
    def output_typespec(self) -> TypeSpec:
        """
        Return the output typespec for the stored pod.
        This is used to validate the output streams.
        """
        return self.pod.output_typespec

    def call(self, tag: dp.Tag, packet: dp.Packet) -> tuple[dp.Tag, dp.Packet | None]:
        return self.pod.call(tag, packet)

    def identity_structure(self, *streams: dp.Stream) -> Any:
        return self.pod.identity_structure(*streams)

    def __repr__(self) -> str:
        return f"StoredPod({self.pod!r})"

    def __str__(self) -> str:
        return f"StoredPod:{self.pod!s}"
