import functools
import logging
import pickle
import warnings
from abc import abstractmethod
import sys
from collections.abc import Callable, Collection, Iterable, Iterator
from typing import (
    Any,
    Literal,
)

from orcabridge.base import Operation
from orcabridge.hashing import get_function_signature, hash_function
from orcabridge.mappers import Join
from orcabridge.store import DataStore, NoOpDataStore
from orcabridge.streams import SyncStream, SyncStreamFromGenerator
from orcabridge.types import Packet, PathSet, PodFunction, Tag

logger = logging.getLogger(__name__)


def function_pod(
    output_keys: Collection[str] | None = None,
    function_name: str | None = None,
    data_store: DataStore | None = None,
    store_name: str | None = None,
    function_hash_mode: Literal["signature", "content", "name", "custom"] = "name",
    custom_hash: int | None = None,
    force_computation: bool = False,
    skip_memoization: bool = False,
    error_handling: Literal["raise", "ignore", "warn"] = "raise",
    **kwargs,
) -> Callable[..., "FunctionPod"]:
    """
    Decorator that wraps a function in a FunctionPod instance.

    Args:
        output_keys: Keys for the function output
        force_computation: Whether to force computation
        skip_memoization: Whether to skip memoization

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

        # Create the FunctionPod
        pod = FunctionPod(
            function=func,
            output_keys=output_keys,
            function_name=function_name or base_function_name,
            data_store=data_store,
            store_name=store_name,
            function_hash_mode=function_hash_mode,
            custom_hash=custom_hash,
            force_computation=force_computation,
            skip_memoization=skip_memoization,
            error_handling=error_handling,
            **kwargs,
        )

        return pod

    return decorator


class Pod(Operation):
    """
    An (abstract) base class for all pods. A pod can be seen as a special type of operation that
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


# TODO: reimplement the memoization as dependency injection


class FunctionPod(Pod):
    """
    A pod that wraps a function and allows it to be used as an operation in a stream.
    This pod can be used to apply a function to the packets in a stream, with optional memoization
    and caching of results. It can also handle multiple output keys and error handling.
    The function should accept keyword arguments that correspond to the keys in the packets.
    The output of the function should be a path or a collection of paths that correspond to the output keys."""

    def __init__(
        self,
        function: PodFunction,
        output_keys: Collection[str] | None = None,
        function_name=None,
        data_store: DataStore | None = None,
        store_name: str | None = None,
        function_hash_mode: Literal["signature", "content", "name", "custom"] = "name",
        custom_hash: int | None = None,
        label: str | None = None,
        force_computation: bool = False,
        skip_memoization_lookup: bool = False,
        skip_memoization: bool = False,
        error_handling: Literal["raise", "ignore", "warn"] = "raise",
        _hash_function_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.function = function
        self.output_keys = output_keys or []
        if function_name is None:
            if hasattr(self.function, "__name__"):
                function_name = getattr(self.function, "__name__")
            else:
                raise ValueError(
                    "function_name must be provided if function has no __name__ attribute"
                )

        self.function_name = function_name
        self.data_store = data_store if data_store is not None else NoOpDataStore()
        self.store_name = store_name or function_name
        self.function_hash_mode = function_hash_mode
        self.custom_hash = custom_hash
        self.force_computation = force_computation
        self.skip_memoization_lookup = skip_memoization_lookup
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

    def is_memoized(self, packet: Packet) -> bool:
        return self.retrieve_memoized(packet) is not None

    def retrieve_memoized(self, packet: Packet) -> Packet | None:
        """
        Retrieve a memoized packet from the data store.
        Returns None if no memoized packet is found.
        """
        return self.data_store.retrieve_memoized(
            self.store_name,
            self.content_hash(char_count=16),
            packet,
        )

    def memoize(
        self,
        packet: Packet,
        output_packet: Packet,
    ) -> Packet:
        """
        Memoize the output packet in the data store.
        Returns the memoized packet.
        """
        return self.data_store.memoize(
            self.store_name,
            self.content_hash(char_count=16),  # identity of this function pod
            packet,
            output_packet,
        )

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
                    if not self.skip_memoization_lookup:
                        memoized_packet = self.retrieve_memoized(packet)
                    else:
                        memoized_packet = None
                    if not self.force_computation and memoized_packet is not None:
                        logger.info("Memoized packet found, skipping computation")
                        yield tag, memoized_packet
                        continue
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
                    output_packet = self.memoize(packet, output_packet)  # type: ignore

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
                name_override=self.function_name,
                function_hash_mode="content",
                content_kwargs=content_kwargs,
            )
        elif self.function_hash_mode == "signature":
            function_hash_value = hash_function(
                self.function,
                name_override=self.function_name,
                function_hash_mode="signature",
                content_kwargs=content_kwargs,
            )
        elif self.function_hash_mode == "name":
            function_hash_value = hash_function(
                self.function,
                name_override=self.function_name,
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
