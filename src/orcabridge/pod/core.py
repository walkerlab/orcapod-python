import functools
import logging
import pickle
import warnings
from abc import abstractmethod
import pyarrow as pa
import sys
from collections.abc import Callable, Collection, Iterable, Iterator, Sequence
from typing import (
    Any,
    Literal,
)

from orcabridge.types.registry import PacketConverter

from orcabridge.core.base import Kernel
from orcabridge.hashing import (
    ObjectHasher,
    ArrowHasher,
    FunctionInfoExtractor,
    get_function_signature,
    hash_function,
    get_default_object_hasher,
    get_default_arrow_hasher,
)
from orcabridge.core.operators import Join
from orcabridge.store import DataStore, ArrowDataStore, NoOpDataStore
from orcabridge.core.streams import SyncStream, SyncStreamFromGenerator
from orcabridge.types import Packet, PathSet, PodFunction, Tag, TypeSpec

from orcabridge.types.default import default_registry
from orcabridge.types.inference import (
    extract_function_data_types,
    verify_against_typespec,
    check_typespec_compatibility,
)
from orcabridge.types.registry import is_packet_supported
import polars as pl

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
        self.error_handling = error_handling

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

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet]: ...

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
                try:
                    tag, output_packet = self.call(tag, packet)
                    n_computed += 1
                    logger.info(f"Computed item {n_computed}")
                    yield tag, output_packet

                except Exception as e:
                    logger.error(f"Error processing packet {packet}: {e}")
                    if self.error_handling == "raise":
                        raise e
                    elif self.error_handling == "ignore":
                        continue
                    elif self.error_handling == "warn":
                        warnings.warn(f"Error processing packet {packet}: {e}")
                        continue

        return SyncStreamFromGenerator(generator)


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
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        stream = self.process_stream(*streams)
        tag_keys, _ = stream[0].keys(trigger_run=trigger_run)
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


def typed_function_pod(
    output_keys: str | Collection[str] | None = None,
    function_name: str | None = None,
    label: str | None = None,
    result_store: ArrowDataStore | None = None,
    tag_store: ArrowDataStore | None = None,
    object_hasher: ObjectHasher | None = None,
    arrow_hasher: ArrowHasher | None = None,
    **kwargs,
) -> Callable[..., "TypedFunctionPod | CachedFunctionPod"]:
    """
    Decorator that wraps a function in a FunctionPod instance.

    Args:
        output_keys: Keys for the function output(s)
        function_name: Name of the function pod; if None, defaults to the function name
        **kwargs: Additional keyword arguments to pass to the FunctionPod constructor. Please refer to the FunctionPod documentation for details.

    Returns:
        FunctionPod instance wrapping the decorated function
    """

    def decorator(func) -> TypedFunctionPod | CachedFunctionPod:
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
        pod = TypedFunctionPod(
            function=func,
            output_keys=output_keys,
            function_name=function_name or base_function_name,
            label=label,
            **kwargs,
        )

        if result_store is not None:
            pod = CachedFunctionPod(
                function_pod=pod,
                object_hasher=object_hasher
                if object_hasher is not None
                else get_default_object_hasher(),
                arrow_hasher=arrow_hasher
                if arrow_hasher is not None
                else get_default_arrow_hasher(),
                result_store=result_store,
                tag_store=tag_store,
            )

        return pod

    return decorator


class TypedFunctionPod(Pod):
    """
    A type-aware pod that wraps a function and provides automatic type validation and inference.

    This pod extends the base Pod functionality by automatically extracting and validating
    type information from function signatures and user-provided specifications. It ensures
    type safety by verifying that both input and output types are supported by the
    configured type registry before execution.

    The TypedFunctionPod analyzes the wrapped function's signature to determine:
    - Parameter types (from annotations or user-provided input_types)
    - Return value types (from annotations or user-provided output_types)
    - Type compatibility with the packet type registry

    Key Features:
    - Automatic type extraction from function annotations
    - Type override support via input_types and output_types parameters
    - Registry-based type validation ensuring data compatibility
    - Memoization support with type-aware caching
    - Multiple output key handling with proper type mapping
    - Comprehensive error handling for type mismatches

    Type Resolution Priority:
    1. User-provided input_types/output_types override function annotations
    2. Function parameter annotations are used when available
    3. Function return annotations are parsed for output type inference
    4. Error raised if types cannot be determined or are unsupported

    Args:
        function: The function to wrap. Must accept keyword arguments corresponding
            to packet keys and return values compatible with output_keys.
        output_keys: Collection of string keys for the function outputs. For functions
            returning a single value, provide a single key. For multiple returns
            (tuple/list), provide keys matching the number of return items.
        function_name: Optional name for the function. Defaults to function.__name__.
        input_types: Optional mapping of parameter names to their types. Overrides
            function annotations for specified parameters.
        output_types: Optional type specification for return values. Can be:
            - A dict mapping output keys to types (TypeSpec)
            - A sequence of types mapped to output_keys in order
            These override inferred types from function return annotations.
        data_store: DataStore instance for memoization. Defaults to NoOpDataStore.
        function_hasher: Hasher function for creating function identity hashes.
            Required parameter - no default implementation available.
        label: Optional label for the pod instance.
        skip_memoization_lookup: If True, skips checking for memoized results.
        skip_memoization: If True, disables memoization entirely.
        error_handling: How to handle execution errors:
            - "raise": Raise exceptions (default)
            - "ignore": Skip failed packets silently
            - "warn": Issue warnings and continue
        packet_type_registry: Registry for validating packet types. Defaults to
            the default registry if None.
        **kwargs: Additional arguments passed to the parent Pod class and above.

    Raises:
        ValueError: When:
            - function_name cannot be determined and is not provided
            - Input types are not supported by the registry
            - Output types are not supported by the registry
            - Type extraction fails due to missing annotations/specifications
        NotImplementedError: When function_hasher is None (required parameter).

    Examples:
        Basic usage with annotated function:

        >>> def process_data(text: str, count: int) -> tuple[str, int]:
        ...     return text.upper(), count * 2
        >>>
        >>> pod = TypedFunctionPod(
        ...     function=process_data,
        ...     output_keys=['upper_text', 'doubled_count'],
        ...     function_hasher=my_hasher
        ... )

        Override types for legacy function:

        >>> def legacy_func(x, y):  # No annotations
        ...     return x + y
        >>>
        >>> pod = TypedFunctionPod(
        ...     function=legacy_func,
        ...     output_keys=['sum'],
        ...     input_types={'x': int, 'y': int},
        ...     output_types={'sum': int},
        ...     function_hasher=my_hasher
        ... )

        Multiple outputs with sequence override:

        >>> def analyze(data: list) -> tuple[int, float, str]:
        ...     return len(data), sum(data), str(data)
        >>>
        >>> pod = TypedFunctionPod(
        ...     function=analyze,
        ...     output_keys=['count', 'total', 'repr'],
        ...     output_types=[int, float, str],  # Override with sequence
        ...     function_hasher=my_hasher
        ... )

    Attributes:
        function: The wrapped function.
        output_keys: List of output key names.
        function_name: Name identifier for the function.
        function_input_types: Resolved input type specification.
        function_output_types: Resolved output type specification.
        registry: Type registry for validation.
        data_store: DataStore instance for memoization.
        function_hasher: Function hasher for identity computation.
        skip_memoization_lookup: Whether to skip memoization lookups.
        skip_memoization: Whether to disable memoization entirely.
        error_handling: Error handling strategy.

    Note:
        The TypedFunctionPod requires a function_hasher to be provided as there
        is no default implementation. This hasher is used to create stable
        identity hashes for memoization and caching purposes.

        Type validation occurs during initialization, ensuring that any type
        incompatibilities are caught early rather than during stream processing.
    """

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
        super().__init__(label=label, **kwargs)
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

        if packet_type_registry is None:
            packet_type_registry = default_registry

        self.registry = packet_type_registry
        self.function_info_extractor = function_info_extractor

        # extract input and output types from the function signature
        function_input_types, function_output_types = extract_function_data_types(
            self.function,
            self.output_keys,
            input_types=input_types,
            output_types=output_types,
        )

        self.function_input_types = function_input_types
        self.function_output_types = function_output_types

        # TODO: include explicit check of support during PacketConverter creation
        self.input_converter = PacketConverter(self.function_input_types, self.registry)
        self.output_converter = PacketConverter(
            self.function_output_types, self.registry
        )

    # TODO: prepare a separate str and repr methods
    def __repr__(self) -> str:
        func_sig = get_function_signature(self.function)
        return f"FunctionPod:{func_sig} ⇒ {self.output_keys}"

    def call(self, tag, packet) -> tuple[Tag, Packet]:
        output_values: list["PathSet"] = []

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

        output_packet: Packet = {k: v for k, v in zip(self.output_keys, output_values)}
        return tag, output_packet

    def identity_structure(self, *streams) -> Any:
        # construct identity structure for the function
        # if function_info_extractor is available, use that but substitute the function_name
        if self.function_info_extractor is not None:
            function_info = self.function_info_extractor.extract_function_info(
                self.function,
                function_name=self.function_name,
                input_types=self.function_input_types,
                output_types=self.function_output_types,
            )
        else:
            # use basic information only
            function_info = {
                "name": self.function_name,
                "input_types": self.function_input_types,
                "output_types": self.function_output_types,
            }
        function_info["output_keys"] = tuple(self.output_keys)

        return (
            self.__class__.__name__,
            function_info,
        ) + tuple(streams)

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        stream = self.process_stream(*streams)
        tag_keys, _ = stream[0].keys(trigger_run=trigger_run)
        return tag_keys, tuple(self.output_keys)


class CachedFunctionPod(Pod):
    def __init__(
        self,
        function_pod: TypedFunctionPod,
        object_hasher: ObjectHasher,
        arrow_hasher: ArrowHasher,
        result_store: ArrowDataStore,
        tag_store: ArrowDataStore | None = None,
        label: str | None = None,
        skip_memoization_lookup: bool = False,
        skip_memoization: bool = False,
        skip_tag_record: bool = False,
        error_handling: Literal["raise", "ignore", "warn"] = "raise",
        **kwargs,
    ) -> None:
        super().__init__(label=label, error_handling=error_handling, **kwargs)
        self.function_pod = function_pod

        self.object_hasher = object_hasher
        self.arrow_hasher = arrow_hasher
        self.result_store = result_store
        self.tag_store = tag_store

        self.skip_memoization_lookup = skip_memoization_lookup
        self.skip_memoization = skip_memoization
        self.skip_tag_record = skip_tag_record

        # TODO: consider making this dynamic
        self.function_pod_hash = self.object_hasher.hash_to_hex(self.function_pod)

    def get_packet_key(self, packet: Packet) -> str:
        return self.arrow_hasher.hash_table(
            self.function_pod.input_converter.to_arrow_table(packet)
        )

    # TODO: prepare a separate str and repr methods
    def __repr__(self) -> str:
        return f"Cached:{self.function_pod}"

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.function_pod.keys(*streams, trigger_run=trigger_run)

    def is_memoized(self, packet: Packet) -> bool:
        return self.retrieve_memoized(packet) is not None

    def add_tag_record(self, tag: Tag, packet: Packet) -> Tag:
        """
        Record the tag for the packet in the record store.
        This is used to keep track of the tags associated with memoized packets.
        """

        return self._add_tag_record_with_packet_key(tag, self.get_packet_key(packet))

    def _add_tag_record_with_packet_key(self, tag: Tag, packet_key: str) -> Tag:
        if self.tag_store is None:
            raise ValueError("Recording of tag requires tag_store but none provided")

        tag = dict(tag)  # ensure we don't modify the original tag
        tag["__packet_key"] = packet_key

        # convert tag to arrow table
        table = pa.Table.from_pylist([tag])

        entry_hash = self.arrow_hasher.hash_table(table)

        # TODO: add error handling
        # check if record already exists:
        retrieved_table = self.tag_store.get_record(
            self.function_pod.function_name, self.function_pod_hash, entry_hash
        )
        if retrieved_table is None:
            self.tag_store.add_record(
                self.function_pod.function_name,
                self.function_pod_hash,
                entry_hash,
                table,
            )

        return tag

    def retrieve_memoized(self, packet: Packet) -> Packet | None:
        """
        Retrieve a memoized packet from the data store.
        Returns None if no memoized packet is found.
        """
        logger.info("Retrieving memoized packet")
        return self._retrieve_memoized_by_hash(self.get_packet_key(packet))

    def _retrieve_memoized_by_hash(self, packet_hash: str) -> Packet | None:
        """
        Retrieve a memoized result packet from the data store, looking up by hash
        Returns None if no memoized packet is found.
        """
        logger.info(f"Retrieving memoized packet with hash {packet_hash}")
        arrow_table = self.result_store.get_record(
            self.function_pod.function_name,
            self.function_pod_hash,
            packet_hash,
        )
        if arrow_table is None:
            return None
        packets = self.function_pod.output_converter.from_arrow_table(arrow_table)
        # since memoizing single packet, it should only contain one packet
        assert len(packets) == 1, (
            f"Memoizing single packet return {len(packets)} packets!"
        )
        return packets[0]

    def memoize(
        self,
        packet: Packet,
        output_packet: Packet,
    ) -> Packet:
        """
        Memoize the output packet in the data store.
        Returns the memoized packet.
        """
        logger.info("Memoizing packet")
        return self._memoize_by_hash(self.get_packet_key(packet), output_packet)

    def _memoize_by_hash(self, packet_hash: str, output_packet: Packet) -> Packet:
        """
        Memoize the output packet in the data store, looking up by hash.
        Returns the memoized packet.
        """
        logger.info(f"Memoizing packet with hash {packet_hash}")
        packets = self.function_pod.output_converter.from_arrow_table(
            self.result_store.add_record(
                self.function_pod.function_name,
                self.function_pod_hash,
                packet_hash,
                self.function_pod.output_converter.to_arrow_table(output_packet),
            )
        )
        # since memoizing single packet, it should only contain one packet
        assert len(packets) == 1, (
            f"Memoizing single packet return {len(packets)} packets!"
        )
        return packets[0]

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet]:
        packet_key = ""
        if (
            not self.skip_tag_record
            or not self.skip_memoization_lookup
            or not self.skip_memoization
        ):
            packet_key = self.get_packet_key(packet)

        if not self.skip_tag_record and self.tag_store is not None:
            self._add_tag_record_with_packet_key(tag, packet_key)

        if not self.skip_memoization_lookup:
            memoized_packet = self._retrieve_memoized_by_hash(packet_key)
        else:
            memoized_packet = None
        if memoized_packet is not None:
            logger.info("Memoized packet found, skipping computation")
            return tag, memoized_packet

        tag, output_packet = self.function_pod.call(tag, packet)

        if not self.skip_memoization:
            # output packet may be modified by the memoization process
            # e.g. if the output is a file, the path may be changed
            output_packet = self.memoize(packet, output_packet)  # type: ignore

        return tag, output_packet

    def get_all_entries_with_tags(self) -> pl.LazyFrame | None:
        """
        Retrieve all entries from the tag store with their associated tags.
        Returns a DataFrame with columns for tag and packet key.
        """
        if self.tag_store is None:
            raise ValueError("Tag store is not set, cannot retrieve entries")

        tag_records = self.tag_store.get_all_records_as_polars(
            self.function_pod.function_name, self.function_pod_hash
        )
        if tag_records is None:
            return None
        result_packets = self.result_store.get_records_by_ids_as_polars(
            self.function_pod.function_name,
            self.function_pod_hash,
            tag_records.collect()["__packet_key"],
            preserve_input_order=True,
        )
        if result_packets is None:
            return None

        return pl.concat([tag_records, result_packets], how="horizontal").drop(
            ["__packet_key"]
        )

    def identity_structure(self, *streams) -> Any:
        return self.function_pod.identity_structure(*streams)
