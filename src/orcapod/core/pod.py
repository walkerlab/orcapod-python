import logging
import warnings
import sys
from collections.abc import Callable, Collection, Iterable, Iterator, Sequence
from typing import (
    Any,
    Literal,
)

from orcapod.types import Packet, Tag, TypeSpec, default_registry
from orcapod.types.typespec_utils import extract_function_typespecs
from orcapod.types.packets import PacketConverter

from orcapod.hashing import (
    FunctionInfoExtractor,
    get_function_signature,
)
from orcapod.core import Kernel
from orcapod.core.operators import Join
from orcapod.core.streams import (
    SyncStream,
    SyncStreamFromGenerator,
)

logger = logging.getLogger(__name__)


class Pod(Kernel):
    """
    An (abstract) base class for all pods. A pod can be seen as a special type of operation that
    only operates on the packet content without reading tags. Consequently, no operation
    of Pod can dependent on the tags of the packets. This is a design choice to ensure that
    the pods act as pure functions which is a necessary condition to guarantee reproducibility.
    """

    def __init__(
        self, error_handling: Literal["raise", "ignore", "warn"] = "raise", **kwargs
    ):
        super().__init__(**kwargs)
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


    def process_stream(self, *streams: SyncStream) -> tuple[SyncStream, ...]:
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

    def pre_forward_hook(
        self, *streams: SyncStream, **kwargs
    ) -> tuple[SyncStream, ...]:
        return self.process_stream(*streams)
    
    def generator_completion_hook(self, n_computed: int) -> None:
        """
        Hook that is called when the generator is completed. This can be used to
        perform any finalization steps, such as closing resources or logging.
        """
        logger.debug(f"Generator completed with {n_computed} items processed.")

    def forward(self, *streams: SyncStream) -> SyncStream:
        # at this point, streams should have been joined into one
        assert len(streams) == 1, "Only one stream is supported in forward() of Pod"
        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            n_computed = 0
            for tag, packet in stream:
                try:
                    tag, output_packet = self.call(tag, packet)
                    if output_packet is None:
                        logger.debug(
                            f"Call returned None as output for tag {tag}. Skipping..."
                        )
                        continue
                    n_computed += 1
                    logger.debug(f"Computed item {n_computed}")
                    yield tag, output_packet

                except Exception as e:
                    logger.error(f"Error processing packet {packet}: {e}")
                    if self.error_handling == "raise":
                        raise e
                    elif self.error_handling == "warn":
                        warnings.warn(f"Error processing packet {packet}: {e}")
                        continue
                    elif self.error_handling == "ignore":
                        continue
                    else:
                        raise ValueError(
                            f"Unknown error handling mode: {self.error_handling} encountered while handling error:"
                        ) from e
            self.generator_completion_hook(n_computed)

        return SyncStreamFromGenerator(generator)

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]: ...


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


class FunctionPod(Pod):
    def __init__(
        self,
        function: Callable[..., Any],
        output_keys: str | Collection[str] | None = None,
        function_name=None,
        input_types: TypeSpec | None = None,
        output_types: TypeSpec | Sequence[type] | None = None,
        label: str | None = None,
        packet_type_registry=None,
        function_info_extractor: FunctionInfoExtractor | None = None,
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

        if packet_type_registry is None:
            # TODO: reconsider the use of default registry here
            packet_type_registry = default_registry

        self.registry = packet_type_registry
        self.function_info_extractor = function_info_extractor

        # extract input and output types from the function signature
        self.function_input_typespec, self.function_output_typespec = (
            extract_function_typespecs(
                self.function,
                self.output_keys,
                input_types=input_types,
                output_types=output_types,
            )
        )

        self.input_converter = PacketConverter(self.function_input_typespec, self.registry)
        self.output_converter = PacketConverter(
            self.function_output_typespec, self.registry
        )

    def get_function_typespecs(self) -> tuple[TypeSpec, TypeSpec]:
        return self.function_input_typespec, self.function_output_typespec


    def __repr__(self) -> str:
        return f"FunctionPod:{self.function!r}"

    def __str__(self) -> str:
        include_module = self.function.__module__ != "__main__"
        func_sig = get_function_signature(self.function, name_override=self.function_name, include_module=include_module)
        return f"FunctionPod:{func_sig}"

    def call(self, tag, packet) -> tuple[Tag, Packet | None]:
        if not self.is_active():
            logger.info(
                f"Pod is not active: skipping computation on input packet {packet}"
            )
            return tag, None
        output_values = []

        values = self.function(**packet)

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

        output_packet: Packet = Packet({k: v for k, v in zip(self.output_keys, output_values)})
        return tag, output_packet

    def identity_structure(self, *streams) -> Any:
        # construct identity structure for the function
        # if function_info_extractor is available, use that but substitute the function_name
        if self.function_info_extractor is not None:
            function_info = self.function_info_extractor.extract_function_info(
                self.function,
                function_name=self.function_name,
                input_typespec=self.function_input_typespec,
                output_typespec=self.function_output_typespec,
            )
        else:
            # use basic information only
            function_info = {
                "name": self.function_name,
                "input_typespec": self.function_input_typespec,
                "output_typespec": self.function_output_typespec,
            }
        function_info["output_keys"] = tuple(self.output_keys)

        return (
            self.__class__.__name__,
            function_info,
        ) + streams

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        stream = self.process_stream(*streams)
        if len(stream) < 1:
            tag_keys = None
        else:
            tag_keys, _ = stream[0].keys(trigger_run=trigger_run)
        return tag_keys, tuple(self.output_keys)

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        stream = self.process_stream(*streams)
        if len(stream) < 1:
            tag_typespec = None
        else:
            tag_typespec, _ = stream[0].types(trigger_run=trigger_run)
        return tag_typespec, self.function_output_typespec

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> bool | None:
        stream = self.process_stream(*streams)
        return stream[0].claims_unique_tags(trigger_run=trigger_run)
