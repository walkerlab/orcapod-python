from typing import (
    Literal,
    Any,
)
from collections.abc import Collection, Iterator
from orcabridge.types import Tag, Packet, PodFunction, PathSet
from orcabridge.hashing import hash_function, get_function_signature
from orcabridge.base import Operation
from orcabridge.stream import SyncStream, SyncStreamFromGenerator
from orcabridge.mapper import Join
from orcabridge.store import DataStore, NoOpDataStore
import functools
import warnings
import logging

logger = logging.getLogger(__name__)


def function_pod(
    output_keys: Collection[str] | None = None,
    store_name: str | None = None,
    data_store: DataStore | None = None,
    function_hash_mode: Literal["signature", "content", "name", "custom"] = "name",
    custom_hash: int | None = None,
    force_computation: bool = False,
    skip_memoization: bool = False,
    error_handling: Literal["raise", "ignore", "warn"] = "raise",
    **kwargs,
):
    """
    Decorator that wraps a function in a FunctionPod instance.

    Args:
        output_keys: Keys for the function output
        force_computation: Whether to force computation
        skip_memoization: Whether to skip memoization

    Returns:
        FunctionPod instance wrapping the decorated function
    """

    def decorator(func):
        # Create a FunctionPod instance with the function and parameters
        pod = FunctionPod(
            function=func,
            output_keys=output_keys,
            store_name=store_name,
            data_store=data_store,
            function_hash_mode=function_hash_mode,
            custom_hash=custom_hash,
            force_computation=force_computation,
            skip_memoization=skip_memoization,
            error_handling=error_handling,
            **kwargs,
        )

        # Update the metadata to make the pod look more like the original function
        functools.update_wrapper(pod, func)

        return pod

    return decorator


class Pod(Operation):
    """
    A base class for all pods. A pod can be seen as a special type of operation that
    only operates on the packet content without reading tags. Consequently, no operation
    of Pod can dependent on the tags of the packets. This is a design choice to ensure that
    the pods act as pure functions which is a necessary condition to guarantee reproducibility.
    """

    def process_stream(self, *streams: SyncStream) -> list[SyncStream]:
        """
        Prepare the incoming streams for execution in the pod. This default implementation
        joins all the streams together and raises and error if no streams are provided.
        """
        # if multiple streams are provided, join them
        # otherwise, return as is
        combined_streams = list(streams)
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join()(stream, next_stream)
            combined_streams = [stream]
        return combined_streams

    def __call__(self, *streams: SyncStream, **kwargs) -> SyncStream:
        stream = self.process_stream(*streams)
        return super().__call__(*stream, **kwargs)

    def forward(self, *streams: SyncStream) -> SyncStream: ...

    def process(self, packet: Packet) -> Packet: ...


# TODO: reimplement the memoization as dependency injection


class FunctionPod(Pod):
    def __init__(
        self,
        function: PodFunction,
        output_keys: Collection[str] | None = None,
        store_name=None,
        data_store: DataStore | None = None,
        function_hash_mode: Literal["signature", "content", "name", "custom"] = "name",
        custom_hash: int | None = None,
        label: str | None = None,
        force_computation: bool = False,
        skip_cache_lookup: bool = False,
        skip_memoization: bool = False,
        error_handling: Literal["raise", "ignore", "warn"] = "raise",
        _hash_function_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.function = function
        if output_keys is None:
            output_keys = []
        self.output_keys = output_keys
        if store_name is None:
            if hasattr(self.function, "__name__"):
                store_name = getattr(self.function, "__name__")
            else:
                raise ValueError(
                    "store_name must be provided if function has no __name__ attribute"
                )

        self.store_name = store_name
        self.data_store = data_store if data_store is not None else NoOpDataStore()
        self.function_hash_mode = function_hash_mode
        self.custom_hash = custom_hash
        self.force_computation = force_computation
        self.skip_cache_lookup = skip_cache_lookup
        self.skip_memoization = skip_memoization
        self.error_handling = error_handling
        self._hash_function_kwargs = _hash_function_kwargs

    def __repr__(self) -> str:
        func_sig = get_function_signature(self.function)
        return f"FunctionPod:{func_sig} ⇒ {self.output_keys}"

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        stream = self.process_stream(*streams)
        tag_keys, _ = stream[0].keys()
        return tag_keys, tuple(self.output_keys)

    def forward(self, *streams: SyncStream) -> SyncStream:
        # if multiple streams are provided, join them
        if len(streams) > 1:
            raise ValueError("Multiple streams should be joined before calling forward")
        if len(streams) == 0:
            raise ValueError("No streams provided to forward")
        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            n_computed = 0
            for tag, packet in stream:
                output_values: list["PathSet"] = []
                try:
                    if not self.skip_cache_lookup:
                        memoized_packet = self.data_store.retrieve_memoized(
                            self.store_name,
                            self.content_hash(char_count=16),
                            packet,
                        )
                    else:
                        memoized_packet = None
                    if not self.force_computation and memoized_packet is not None:
                        logger.info("Memoized packet found, skipping computation")
                        yield tag, memoized_packet
                        continue
                    values = self.function(**packet)

                    if len(self.output_keys) == 0:
                        output_values: list["PathSet"] = []
                    elif (
                        len(self.output_keys) == 1
                        and values is not None
                        and not isinstance(values, Collection)
                    ):
                        output_values = [values]
                    elif isinstance(values, Collection):
                        output_values = list(values)  # type: ignore
                    elif len(self.output_keys) > 1:
                        raise ValueError(
                            "Values returned by function must be a pathlike or a sequence of pathlikes"
                        )

                    if len(output_values) != len(self.output_keys):
                        raise ValueError(
                            "Number of output keys does not match number of values returned by function"
                        )
                except Exception as e:
                    logger.error(f"Error processing packet {packet}: {e}")
                    if self.error_handling == "raise":
                        raise e
                    elif self.error_handling == "ignore":
                        continue
                    elif self.error_handling == "warn":
                        warnings.warn(f"Error processing packet {packet}: {e}")
                        continue

                output_packet: Packet = {
                    k: v for k, v in zip(self.output_keys, output_values)
                }

                if not self.skip_memoization:
                    # output packet may be modified by the memoization process
                    # e.g. if the output is a file, the path may be changed
                    output_packet = self.data_store.memoize(
                        self.store_name,
                        self.content_hash(),  # identity of this function pod
                        packet,
                        output_packet,
                    )

                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet

        return SyncStreamFromGenerator(generator)

    def identity_structure(self, *streams) -> Any:
        content_kwargs = self._hash_function_kwargs
        if self.function_hash_mode == "content":
            if content_kwargs is None:
                content_kwargs = {
                    "include_name": False,
                    "include_module": False,
                    "include_declaration": False,
                }
            function_hash_value = hash_function(
                self.function,
                function_hash_mode="content",
                content_kwargs=content_kwargs,
            )
        elif self.function_hash_mode == "signature":
            function_hash_value = hash_function(
                self.function,
                function_hash_mode="signature",
                content_kwargs=content_kwargs,
            )
        elif self.function_hash_mode == "name":
            function_hash_value = hash_function(
                self.function,
                function_hash_mode="name",
                content_kwargs=content_kwargs,
            )
        elif self.function_hash_mode == "custom":
            if self.custom_hash is None:
                raise ValueError("Custom hash function not provided")
            function_hash_value = self.custom_hash
        else:
            raise ValueError(
                f"Unknown function hash mode: {self.function_hash_mode}. "
                "Must be one of 'content', 'signature', 'name', or 'custom'."
            )

        return (
            self.__class__.__name__,
            function_hash_value,
            tuple(self.output_keys),
        ) + tuple(streams)
