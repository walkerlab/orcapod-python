from orcapod.core.pod import Pod, FunctionPod
from orcapod.core import SyncStream, Source, Kernel
from orcapod.store import ArrowDataStore
from orcapod.types import Tag, Packet, TypeSpec, default_registry
from orcapod.types.typespec import extract_function_typespecs
from orcapod.hashing import ObjectHasher, ArrowHasher
from orcapod.hashing.defaults import get_default_object_hasher, get_default_arrow_hasher
from typing import Any, Literal
from collections.abc import Collection, Iterator
from orcapod.types.registry import TypeRegistry, PacketConverter
import pyarrow as pa
import polars as pl
from orcapod.core.streams import SyncStreamFromGenerator
from orcapod.utils.stream_utils import get_typespec, union_typespecs

import logging

logger = logging.getLogger(__name__)


def tag_to_arrow_table_with_metadata(tag, metadata: dict | None = None):
    """
    Convert a tag dictionary to PyArrow table with metadata on each column.

    Args:
        tag: Dictionary with string keys and any Python data type values
        metadata_key: The metadata key to add to each column
        metadata_value: The metadata value to indicate this column came from tag
    """
    if metadata is None:
        metadata = {}

    # First create the table to infer types
    temp_table = pa.Table.from_pylist([tag])

    # Create new fields with metadata
    fields_with_metadata = []
    for field in temp_table.schema:
        # Add metadata to each field
        field_metadata = metadata
        new_field = pa.field(
            field.name, field.type, nullable=field.nullable, metadata=field_metadata
        )
        fields_with_metadata.append(new_field)

    # Create schema with metadata
    schema_with_metadata = pa.schema(fields_with_metadata)

    # Create the final table with the metadata-enriched schema
    table = pa.Table.from_pylist([tag], schema=schema_with_metadata)

    return table


def get_columns_with_metadata(
    df: pl.DataFrame, key: str, value: str | None = None
) -> list[str]:
    """Get column names with specific metadata using list comprehension. If value is given, only
    columns matching that specific value for the desginated metadata key will be returned.
    Otherwise, all columns that contains the key as metadata will be returned regardless of the value"""
    return [
        col_name
        for col_name, dtype in df.schema.items()
        if hasattr(dtype, "metadata")
        and (value is None or getattr(dtype, "metadata") == value)
    ]


class PolarsSource(Source):
    def __init__(self, df: pl.DataFrame, tag_keys: Collection[str] | None = None):
        self.df = df
        self.tag_keys = tag_keys

    def forward(self, *streams: SyncStream, **kwargs) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "PolarsSource does not support forwarding streams. "
                "It generates its own stream from the DataFrame."
            )
        return PolarsStream(self.df, self.tag_keys)


class PolarsStream(SyncStream):
    def __init__(self, df: pl.DataFrame, tag_keys: Collection[str] | None = None):
        self.df = df
        if tag_keys is None:
            # extract tag_keys by picking columns with metadata source=tag
            tag_keys = get_columns_with_metadata(df, "source", "tag")
        self.tag_keys = tag_keys

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        for row in self.df.iter_rows(named=True):
            tag = {key: row[key] for key in self.tag_keys}
            packet = {key: val for key, val in row.items() if key not in self.tag_keys}
            yield tag, packet


class EmptyStream(SyncStream):
    def __init__(
        self,
        tag_keys: Collection[str] | None = None,
        packet_keys: Collection[str] | None = None,
        tag_typespec: TypeSpec | None = None,
        packet_typespec: TypeSpec | None = None,
    ):
        if tag_keys is None and tag_typespec is not None:
            tag_keys = tag_typespec.keys()
        self.tag_keys = list(tag_keys) if tag_keys else []

        if packet_keys is None and packet_typespec is not None:
            packet_keys = packet_typespec.keys()
        self.packet_keys = list(packet_keys) if packet_keys else []

        self.tag_typespec = tag_typespec
        self.packet_typespec = packet_typespec

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.tag_keys, self.packet_keys

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        return self.tag_typespec, self.packet_typespec

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        # Empty stream, no data to yield
        return iter([])


class KernelInvocationWrapper(Kernel):
    def __init__(
        self, kernel: Kernel, input_streams: Collection[SyncStream], **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.kernel = kernel
        self.input_streams = list(input_streams)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self.kernel!r}>"

    def __str__(self):
        return f"{self.__class__.__name__}<{self.kernel}>"

    def computed_label(self) -> str | None:
        """
        Return the label of the wrapped kernel.
        """
        return self.kernel.label

    def resolve_input_streams(self, *input_streams) -> Collection[SyncStream]:
        if input_streams:
            raise ValueError(
                "Wrapped pod with specified streams cannot be invoked with additional streams"
            )
        return self.input_streams

    def identity_structure(self, *streams: SyncStream) -> Any:
        """
        Identity structure that includes the wrapped kernel's identity structure.
        """
        resolved_streams = self.resolve_input_streams(*streams)
        return self.kernel.identity_structure(*resolved_streams)

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        resolved_streams = self.resolve_input_streams(*streams)
        return self.kernel.keys(*resolved_streams, trigger_run=trigger_run)

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        resolved_streams = self.resolve_input_streams(*streams)
        return self.kernel.types(*resolved_streams, trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> bool | None:
        resolved_streams = self.resolve_input_streams(*streams)
        return self.kernel.claims_unique_tags(
            *resolved_streams, trigger_run=trigger_run
        )


class CachedKernelWrapper(KernelInvocationWrapper, Source):
    """
    A Kernel wrapper that wraps a kernel and stores the outputs of the kernel.
    If the class is instantiated with input_streams that is not None, then this wrapper
    will strictly represent the invocation of the wrapped Kernel on the given input streams.
    Passing in an empty list into input_streams would still be registered as a specific invocation.
    If input_streams is None, the class instance largely acts as a proxy of the underlying kernel
    but will try to save all results. Note that depending on the storage type passed in, the saving
    may error out if you invoke the instance on input streams with non-compatible schema (e.g., tags with
    different keys).
    """

    def __init__(
        self,
        kernel: Kernel,
        input_streams: Collection[SyncStream],
        output_store: ArrowDataStore,
        kernel_hasher: ObjectHasher | None = None,
        arrow_packet_hasher: ArrowHasher | None = None,
        packet_type_registry: TypeRegistry | None = None,
        **kwargs,
    ) -> None:
        super().__init__(kernel, input_streams, **kwargs)

        self.output_store = output_store

        # These are configurable but are not expected to be modified except for special circumstances
        if kernel_hasher is None:
            kernel_hasher = get_default_object_hasher()
        self._kernel_hasher = kernel_hasher
        if arrow_packet_hasher is None:
            arrow_packet_hasher = get_default_arrow_hasher()
        self._arrow_packet_hasher = arrow_packet_hasher
        if packet_type_registry is None:
            packet_type_registry = default_registry
        self._packet_type_registry = packet_type_registry

        self.source_info = self.label, self.kernel_hasher.hash_to_hex(self.kernel)
        self.tag_keys, self.packet_keys = self.keys(trigger_run=False)
        self.output_converter = None

        self._cache_computed = False

    @property
    def arrow_hasher(self):
        return self._arrow_packet_hasher

    @property
    def registry(self):
        return self._packet_type_registry

    @property
    def kernel_hasher(self) -> ObjectHasher:
        if self._kernel_hasher is None:
            return get_default_object_hasher()
        return self._kernel_hasher

    @kernel_hasher.setter
    def kernel_hasher(self, kernel_hasher: ObjectHasher | None = None):
        if kernel_hasher is None:
            kernel_hasher = get_default_object_hasher()
        self._kernel_hasher = kernel_hasher
        # hasher changed -- trigger recomputation of properties that depend on kernel hasher
        self.update_cached_values()

    def update_cached_values(self):
        self.source_info = self.label, self.kernel_hasher.hash_to_hex(self.kernel)
        self.tag_keys, self.packet_keys = self.keys(trigger_run=False)
        self.output_converter = None

    def forward(self, *streams: SyncStream, **kwargs) -> SyncStream:
        if self._cache_computed:
            logger.info(f"Returning cached outputs for {self}")
            if self.df is not None:
                return PolarsStream(self.df, tag_keys=self.tag_keys)
            else:
                return EmptyStream(tag_keys=self.tag_keys, packet_keys=self.packet_keys)

        resolved_streams = self.resolve_input_streams(*streams)

        output_stream = self.kernel.forward(*resolved_streams, **kwargs)

        tag_type, packet_type = output_stream.types(trigger_run=False)
        if tag_type is not None and packet_type is not None:
            joined_type = union_typespecs(tag_type, packet_type)
            assert joined_type is not None, "Joined typespec should not be None"
            self.output_converter = PacketConverter(joined_type, registry=self.registry)

        # Cache the output stream of the underlying kernel
        # This is a no-op if the output stream is already cached
        def generator() -> Iterator[tuple[Tag, Packet]]:
            logger.info(f"Computing and caching outputs for {self}")
            for tag, packet in output_stream:
                merged_info = {**tag, **packet}
                if self.output_converter is None:
                    joined_type = get_typespec(merged_info)
                    assert joined_type is not None, "Joined typespec should not be None"
                    self.output_converter = PacketConverter(
                        joined_type, registry=self.registry
                    )

                output_table = self.output_converter.to_arrow_table(merged_info)
                output_id = self.arrow_hasher.hash_table(output_table)
                if not self.output_store.get_record(*self.source_info, output_id):
                    self.output_store.add_record(
                        *self.source_info,
                        output_id,
                        output_table,
                    )
                yield tag, packet
            self._cache_computed = True

        return SyncStreamFromGenerator(generator)

    @property
    def lazy_df(self) -> pl.LazyFrame | None:
        return self.output_store.get_all_records_as_polars(*self.source_info)

    @property
    def df(self) -> pl.DataFrame | None:
        lazy_df = self.lazy_df
        if lazy_df is None:
            return None
        return lazy_df.collect()

    def reset_cache(self):
        self._cache_computed = False


class FunctionPodInvocationWrapper(KernelInvocationWrapper, Pod):
    """
    Convenience class to wrap a function pod, providing default pass-through
    implementations
    """

    def __init__(
        self, function_pod: FunctionPod, input_streams: Collection[SyncStream], **kwargs
    ):
        # note that this would be an alias to the self.kernel but here explicitly taken as function_pod
        # for better type hints
        # MRO will be KernelInvocationWrapper -> Pod -> Kernel
        super().__init__(function_pod, input_streams, **kwargs)
        self.function_pod = function_pod

    def forward(self, *streams: SyncStream, **kwargs) -> SyncStream:
        resolved_streams = self.resolve_input_streams(*streams)
        return super().forward(*resolved_streams, **kwargs)

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        return self.function_pod.call(tag, packet)

    # =============pass through methods/properties to the underlying function pod=============

    def set_active(self, active=True):
        """
        Set the active state of the function pod.
        """
        self.function_pod.set_active(active)

    def is_active(self) -> bool:
        """
        Check if the function pod is active.
        """
        return self.function_pod.is_active()


class CachedFunctionPodWrapper(FunctionPodInvocationWrapper, Source):
    def __init__(
        self,
        function_pod: FunctionPod,
        input_streams: Collection[SyncStream],
        output_store: ArrowDataStore,
        tag_store: ArrowDataStore | None = None,
        label: str | None = None,
        skip_memoization_lookup: bool = False,
        skip_memoization: bool = False,
        skip_tag_record: bool = False,
        error_handling: Literal["raise", "ignore", "warn"] = "raise",
        object_hasher: ObjectHasher | None = None,
        arrow_hasher: ArrowHasher | None = None,
        registry: TypeRegistry | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            function_pod,
            input_streams,
            label=label,
            error_handling=error_handling,
            **kwargs,
        )
        self.output_store = output_store
        self.tag_store = tag_store

        self.skip_memoization_lookup = skip_memoization_lookup
        self.skip_memoization = skip_memoization
        self.skip_tag_record = skip_tag_record

        # These are configurable but are not expected to be modified except for special circumstances
        # Here I'm assigning to the hidden properties directly to avoid triggering setters
        if object_hasher is None:
            object_hasher = get_default_object_hasher()
        self._object_hasher = object_hasher
        if arrow_hasher is None:
            arrow_hasher = get_default_arrow_hasher()
        self._arrow_hasher = arrow_hasher
        if registry is None:
            registry = default_registry
        self._registry = registry

        # compute and cache properties and converters for efficiency
        self.update_cached_values()
        self._cache_computed = False

    @property
    def object_hasher(self) -> ObjectHasher:
        return self._object_hasher

    @object_hasher.setter
    def object_hasher(self, object_hasher: ObjectHasher | None = None):
        if object_hasher is None:
            object_hasher = get_default_object_hasher()
        self._object_hasher = object_hasher
        # hasher changed -- trigger recomputation of properties that depend on object hasher
        self.update_cached_values()

    @property
    def arrow_hasher(self) -> ArrowHasher:
        return self._arrow_hasher

    @arrow_hasher.setter
    def arrow_hasher(self, arrow_hasher: ArrowHasher | None = None):
        if arrow_hasher is None:
            arrow_hasher = get_default_arrow_hasher()
        self._arrow_hasher = arrow_hasher
        # hasher changed -- trigger recomputation of properties that depend on arrow hasher
        self.update_cached_values()

    @property
    def registry(self) -> TypeRegistry:
        return self._registry

    @registry.setter
    def registry(self, registry: TypeRegistry | None = None):
        if registry is None:
            registry = default_registry
        self._registry = registry
        # registry changed -- trigger recomputation of properties that depend on registry
        self.update_cached_values()

    def update_cached_values(self) -> None:
        self.function_pod_hash = self.object_hasher.hash_to_hex(self.function_pod)
        self.tag_keys, self.output_keys = self.keys(trigger_run=False)
        self.input_typespec, self.output_typespec = (
            self.function_pod.get_function_typespecs()
        )
        self.input_converter = PacketConverter(self.input_typespec, self.registry)
        self.output_converter = PacketConverter(self.output_typespec, self.registry)

    def reset_cache(self):
        self._cache_computed = False

    def generator_completion_hook(self, n_computed: int) -> None:
        """
        Hook to be called when the generator is completed.
        """
        logger.info(f"Results cached for {self}")
        self._cache_computed = True

    def forward(self, *streams: SyncStream, **kwargs) -> SyncStream:
        if self._cache_computed:
            logger.info(f"Returning cached outputs for {self}")
            if self.df is not None:
                return PolarsStream(self.df, self.tag_keys)
            else:
                return EmptyStream(tag_keys=self.tag_keys, packet_keys=self.output_keys)
        logger.info(f"Computing and caching outputs for {self}")
        return super().forward(*streams, **kwargs)

    def get_packet_key(self, packet: Packet) -> str:
        # TODO: reconsider the logic around input/output converter -- who should own this?
        return self.arrow_hasher.hash_table(self.input_converter.to_arrow_table(packet))

    @property
    def source_info(self):
        return self.function_pod.function_name, self.function_pod_hash

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

        # TODO: consider making this more efficient
        # convert tag to arrow table - columns are labeled with metadata source=tag
        table = tag_to_arrow_table_with_metadata(tag, {"source": "tag"})

        entry_hash = self.arrow_hasher.hash_table(table)

        # TODO: add error handling
        # check if record already exists:
        retrieved_table = self.tag_store.get_record(*self.source_info, entry_hash)
        if retrieved_table is None:
            self.tag_store.add_record(*self.source_info, entry_hash, table)

        return tag

    def retrieve_memoized(self, packet: Packet) -> Packet | None:
        """
        Retrieve a memoized packet from the data store.
        Returns None if no memoized packet is found.
        """
        logger.debug("Retrieving memoized packet")
        return self._retrieve_memoized_with_packet_key(self.get_packet_key(packet))

    def _retrieve_memoized_with_packet_key(self, packet_key: str) -> Packet | None:
        """
        Retrieve a memoized result packet from the data store, looking up by the packet key
        Returns None if no memoized packet is found.
        """
        logger.debug(f"Retrieving memoized packet with key {packet_key}")
        arrow_table = self.output_store.get_record(
            self.function_pod.function_name,
            self.function_pod_hash,
            packet_key,
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
        logger.debug("Memoizing packet")
        return self._memoize_with_packet_key(self.get_packet_key(packet), output_packet)

    def _memoize_with_packet_key(
        self, packet_key: str, output_packet: Packet
    ) -> Packet:
        """
        Memoize the output packet in the data store, looking up by packet key.
        Returns the memoized packet.
        """
        logger.debug(f"Memoizing packet with key {packet_key}")
        # TODO: this logic goes through the entire store and retrieve cycle with two conversions
        # consider simpler alternative
        packets = self.output_converter.from_arrow_table(
            self.output_store.add_record(
                self.function_pod.function_name,
                self.function_pod_hash,
                packet_key,
                self.output_converter.to_arrow_table(output_packet),
            )
        )
        # since passed in a single packet, it should only return a single packet
        assert len(packets) == 1, (
            f"Memoizing single packet returned {len(packets)} packets!"
        )
        return packets[0]

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        packet_key = ""
        if (
            not self.skip_tag_record
            or not self.skip_memoization_lookup
            or not self.skip_memoization
        ):
            packet_key = self.get_packet_key(packet)

        output_packet = None
        if not self.skip_memoization_lookup:
            output_packet = self._retrieve_memoized_with_packet_key(packet_key)
            if output_packet is not None:
                logger.debug(
                    f"Memoized output for {packet} with {packet_key} found, skipping computation"
                )
            else:
                logger.debug(
                    f"Memoized output for packet {packet} with {packet_key} not found"
                )

        if output_packet is None:
            # TODO: revisit the logic around active state and how to use it
            tag, output_packet = self.function_pod.call(tag, packet)
            if output_packet is not None and not self.skip_memoization:
                # output packet may be modified by the memoization process
                # e.g. if the output is a file, the path may be changed
                output_packet = self._memoize_with_packet_key(packet_key, output_packet)  # type: ignore

        if output_packet is None:
            if self.is_active():
                logger.warning(
                    f"Function pod {self.function_pod.function_name} returned None for packet {packet} despite being active"
                )
            return tag, None

        # result was successfully computed -- save the tag
        if not self.skip_tag_record and self.tag_store is not None:
            self._add_tag_record_with_packet_key(tag, packet_key)

        return tag, output_packet

    def get_all_outputs(self) -> pl.LazyFrame | None:
        return self.output_store.get_all_records_as_polars(*self.source_info)

    def get_all_tags(self, with_packet_id: bool = False) -> pl.LazyFrame | None:
        if self.tag_store is None:
            raise ValueError("Tag store is not set, no tag record can be retrieved")
        data = self.tag_store.get_all_records_as_polars(*self.source_info)
        if not with_packet_id:
            return data.drop("__packet_key") if data is not None else None
        return data

    def get_all_entries_with_tags(self) -> pl.LazyFrame | None:
        """
        Retrieve all entries from the tag store with their associated tags.
        Returns a DataFrame with columns for tag and packet key.
        """
        if self.tag_store is None:
            raise ValueError("Tag store is not set, no tag record can be retrieved")

        tag_records = self.tag_store.get_all_records_as_polars(*self.source_info)
        if tag_records is None:
            return None
        result_packets = self.output_store.get_records_by_ids_as_polars(
            *self.source_info,
            tag_records.collect()["__packet_key"],
            preserve_input_order=True,
        )
        if result_packets is None:
            return None

        return pl.concat([tag_records, result_packets], how="horizontal").drop(
            ["__packet_key"]
        )

    @property
    def df(self) -> pl.DataFrame | None:
        lazy_df = self.lazy_df
        if lazy_df is None:
            return None
        return lazy_df.collect()

    @property
    def lazy_df(self) -> pl.LazyFrame | None:
        return self.get_all_entries_with_tags()

    @property
    def tags(self) -> pl.DataFrame | None:
        data = self.get_all_tags()
        if data is None:
            return None

        return data.collect()

    @property
    def outputs(self) -> pl.DataFrame | None:
        """
        Retrieve all outputs from the result store as a DataFrame.
        Returns None if no outputs are available.
        """
        data = self.get_all_outputs()
        if data is None:
            return None

        return data.collect()


class DummyFunctionPod(Pod):
    def __init__(self, function_name="dummy", **kwargs):
        super().__init__(**kwargs)
        self.function_name = function_name

    def set_active(self, active: bool = True):
        # no-op
        pass

    def is_active(self) -> bool:
        return False

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        raise NotImplementedError(
            "DummyFunctionPod cannot be called, it is only used to access previously stored tags and outputs."
        )


# TODO: Create this instead using compositional pattern
class DummyCachedFunctionPod(CachedFunctionPodWrapper):
    """
    Dummy for a cached function pod. This is convenient to just allow the user to access
    previously stored function pod tags and outputs without requiring instantiating the identical
    function used for computation.

    Consequently, this function pod CANNOT be used to compute and insert new entries into the storage.
    """

    def __init__(self, source_pod: CachedFunctionPodWrapper):
        self._source_info = source_pod.source_info
        self.output_store = source_pod.output_store
        self.tag_store = source_pod.tag_store
        self.function_pod = DummyFunctionPod(source_pod.function_pod.function_name)

    @property
    def source_info(self) -> tuple[str, str]:
        return self._source_info


class Node(KernelInvocationWrapper, Source):
    def __init__(self, kernel: Kernel, input_nodes: Collection["Node"], **kwargs):
        """
        Create a node that wraps a kernel and provides a Node interface.
        This is useful for creating nodes in a pipeline that can be executed.
        """
        return super().__init__(kernel, input_nodes, **kwargs)

    def reset_cache(self) -> None: ...


class KernelNode(Node, CachedKernelWrapper):
    """
    A node that wraps a Kernel and provides a Node interface.
    This is useful for creating nodes in a pipeline that can be executed.
    """


class FunctionPodNode(Node, CachedFunctionPodWrapper):
    """
    A node that wraps a FunctionPod and provides a Node interface.
    This is useful for creating nodes in a pipeline that can be executed.
    """
