import logging

logger = logging.getLogger(__name__)

from pathlib import Path
from typing import List, Optional, Tuple, Iterator, Iterable, Collection
from .utils.hash import hash_dict
from .utils.name import get_function_signature, function_content_hash
from .base import Operation
from .mapper import Join
from .stream import SyncStream, SyncStreamFromGenerator
from .types import Tag, Packet, PodFunction
from .store import DataStore, NoOpDataStore
import json
import shutil
import functools


def function_pod(
    output_keys: Optional[Collection[str]] = None,
    store_name: Optional[str] = None,
    data_store: Optional[DataStore] = None,
    force_computation: bool = False,
    skip_memoization: bool = False,
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
            store_name=store_name,
            data_store=data_store,
            output_keys=output_keys,
            force_computation=force_computation,
            skip_memoization=skip_memoization,
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

    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        The forward method is the main entry point for the pod. It takes a stream of packets
        and returns a stream of packets.
        """
        # if multiple streams are provided, join them
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join()(stream, next_stream)
        elif len(streams) == 1:
            stream = streams[0]
        else:
            raise ValueError("No streams provided to FunctionPod")

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            n_computed = 0
            for tag, packet in stream:
                output_packet = self.process(packet)
                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet

        return SyncStreamFromGenerator(generator)

    def process(self, packet: Packet) -> Packet: ...


# TODO: reimplement the memoization as dependency injection


class FunctionPod(Pod):
    def __init__(
        self,
        function: PodFunction,
        output_keys: Optional[Collection[str]] = None,
        store_name=None,
        data_store: Optional[DataStore] = None,
        force_computation: bool = False,
        skip_memoization: bool = False,
    ) -> None:
        super().__init__()
        self.function = function
        if output_keys is None:
            output_keys = []
        self.output_keys = output_keys
        self.store_name = self.function.__name__ if store_name is None else store_name
        self.data_store = data_store if data_store is not None else NoOpDataStore()
        self.skip_memoization = skip_memoization
        self.force_computation = force_computation

    def __repr__(self) -> str:
        func_sig = get_function_signature(self.function)
        return f"FunctionPod:{func_sig} ⇒ {self.output_keys}"

    def __hash__(self) -> int:
        function_hash = function_content_hash(self.function)
        return hash((function_hash, tuple(self.output_keys)))

    def forward(self, *streams: SyncStream) -> SyncStream:
        # if multiple streams are provided, join them
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join()(stream, next_stream)
        elif len(streams) == 1:
            stream = streams[0]
        else:
            raise ValueError("No streams provided to FunctionPod")

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            n_computed = 0
            for tag, packet in stream:
                memoized_packet = self.data_store.retrieve_memoized(
                    self.store_name, packet
                )
                if not self.force_computation and memoized_packet is not None:
                    yield tag, memoized_packet
                    continue
                values = self.function(**packet)
                if len(self.output_keys) == 0:
                    values = []
                elif len(self.output_keys) == 1:
                    values = [values]
                elif isinstance(values, Iterable):
                    values = list(values)
                elif len(self.output_keys) > 1:
                    raise ValueError(
                        "Values returned by function must be a pathlike or a sequence of pathlikes"
                    )

                if len(values) != len(self.output_keys):
                    raise ValueError(
                        "Number of output keys does not match number of values returned by function"
                    )

                output_packet: Packet = {k: v for k, v in zip(self.output_keys, values)}

                if not self.skip_memoization:
                    # output packet may be modified by the memoization process
                    # e.g. if the output is a file, the path may be changed
                    output_packet = self.data_store.memoize(
                        self.store_name, packet, output_packet
                    )

                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet

        return SyncStreamFromGenerator(generator)

    def memoize(
        self, packet: Packet, output_packet: Packet, overwrite: bool = False
    ) -> Packet:
        return output_packet

    def retrieve_memoized(self, packet: Packet) -> Optional[Packet]:
        return None
