from collections import defaultdict
from collections.abc import Callable, Collection, Iterator
from itertools import chain
from typing import Any


from orcabridge.base import Mapper, SyncStream
from orcabridge.hashing import function_content_hash, hash_function
from orcabridge.streams import SyncStreamFromGenerator
from orcabridge.utils.stream_utils import (
    batch_packet,
    batch_tags,
    check_packet_compatibility,
    join_tags,
)
from orcabridge.utils.stream_utils import fill_missing

from .types import Packet, Tag


class Repeat(Mapper):
    """
    A Mapper that repeats the packets in the stream a specified number of times.
    The repeat count is the number of times to repeat each packet.
    """

    def __init__(self, repeat_count: int) -> None:
        super().__init__()
        if not isinstance(repeat_count, int):
            raise TypeError("repeat_count must be an integer")
        if repeat_count < 0:
            raise ValueError("repeat_count must be non-negative")
        self.repeat_count = repeat_count

    def identity_structure(self, *streams) -> tuple[str, int, set[SyncStream]]:
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, self.repeat_count, set(streams))

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Repeat does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Repeat operation requires exactly one stream")

        stream = streams[0]
        return stream.keys()

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("Repeat operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in stream:
                for _ in range(self.repeat_count):
                    yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"Repeat(count={self.repeat_count})"

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = True
    ) -> bool:
        if len(streams) != 1:
            raise ValueError(
                "Repeat operation only supports operating on a single input stream"
            )

        # Repeat's uniquness is true only if (1) input stream has unique tags and (2) repeat count is 1
        return self.repeat_count == 1 and streams[0].claims_unique_tags(
            trigger_run=trigger_run
        )


class Merge(Mapper):
    def identity_structure(self, *streams):
        # Merge does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Merge does not alter the keys of the stream.
        """
        if len(streams) < 2:
            raise ValueError("Merge operation requires at least two streams")

        merged_tag_keys = set()
        merged_packet_keys = set()

        for stream in streams:
            tag_keys, packet_keys = stream.keys()
            if tag_keys is not None:
                merged_tag_keys.update(set(tag_keys))
            if packet_keys is not None:
                merged_packet_keys.update(set(packet_keys))

        return list(merged_tag_keys), list(merged_packet_keys)

    def forward(self, *streams: SyncStream) -> SyncStream:
        tag_keys, packet_keys = self.keys(*streams)

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in chain(*streams):
                # fill missing keys with None
                tag = fill_missing(tag, tag_keys)
                packet = fill_missing(packet, packet_keys)
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "Merge()"

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = True
    ) -> bool:
        """
        Merge operation can only claim unique tags if all input streams have unique tags AND
        the tag keys are not identical across all streams.
        """
        if len(streams) < 2:
            raise ValueError("Merge operation requires at least two streams")
        # Check if all streams have unique tags
        unique_tags = all(
            stream.claims_unique_tags(trigger_run=trigger_run) for stream in streams
        )
        if not unique_tags:
            return False
        # check that all streams' tag keys are not identical
        tag_key_pool = set()
        for stream in streams:
            tag_keys, packet_keys = stream.keys()
            # TODO: re-evaluate the implication of having empty tag keys in uniqueness guarantee
            if tag_keys is None or set(tag_keys) in tag_key_pool:
                return False
            tag_key_pool.add(frozenset(tag_keys))

        return True


class Join(Mapper):
    def identity_structure(self, *streams):
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(self, *streams: SyncStream) -> tuple[Collection[str], Collection[str]]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned if it is feasible to do so, otherwise a tuple
        (None, None) is returned to signify that the keys are not known.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_keys, left_packet_keys = left_stream.keys()
        right_tag_keys, right_packet_keys = right_stream.keys()

        joined_tag_keys = list(set(left_tag_keys or []) | set(right_tag_keys or []))
        joined_packet_keys = list(
            set(left_packet_keys or []) | set(right_packet_keys or [])
        )

        return joined_tag_keys, joined_packet_keys

    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams

        def generator():
            for left_tag, left_packet in left_stream:
                for right_tag, right_packet in right_stream:
                    if (joined_tag := join_tags(left_tag, right_tag)) is not None:
                        if not check_packet_compatibility(left_packet, right_packet):
                            raise ValueError(
                                f"Packets are not compatible: {left_packet} and {right_packet}"
                            )
                        yield joined_tag, {**left_packet, **right_packet}

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "Join()"


class FirstMatch(Mapper):
    def identity_structure(self, *streams: SyncStream) -> tuple[str, set[SyncStream]]:
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned if it is feasible to do so, otherwise a tuple
        (None, None) is returned to signify that the keys are not known.
        """
        if len(streams) != 2:
            raise ValueError("FirstMatch operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_keys, left_packet_keys = left_stream.keys()
        right_tag_keys, right_packet_keys = right_stream.keys()

        joined_tag_keys = list(set(left_tag_keys or []) | set(right_tag_keys or []))
        joined_packet_keys = list(
            set(left_packet_keys or []) | set(right_packet_keys or [])
        )

        return joined_tag_keys, joined_packet_keys

    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) != 2:
            raise ValueError("MatchUpToN operation requires exactly two streams")

        left_stream, right_stream = streams

        # get all elements from both streams
        outer_stream = list(left_stream)
        inner_stream = list(right_stream)

        # take the longer one as the outer stream
        if len(outer_stream) < len(inner_stream):
            # swap the stream
            outer_stream, inner_stream = inner_stream, outer_stream

        # only finds up to one possible match for each packet
        def generator():
            for outer_tag, outer_packet in outer_stream:
                for idx, (inner_tag, inner_packet) in enumerate(inner_stream):
                    if (joined_tag := join_tags(outer_tag, inner_tag)) is not None:
                        if not check_packet_compatibility(outer_packet, inner_packet):
                            raise ValueError(
                                f"Packets are not compatible: {outer_packet} and {inner_packet}"
                            )
                        # match is found - remove the packet from the inner stream
                        inner_stream.pop(idx)
                        yield joined_tag, {**outer_packet, **inner_packet}
                        # if enough matches found, move onto the next outer stream packet
                        break

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "MatchUpToN()"


class MapPackets(Mapper):
    """
    A Mapper that maps the keys of the packet in the stream to new keys.
    The mapping is done using a dictionary that maps old keys to new keys.
    If a key is not in the mapping, it will be dropped from the element unless
    drop_unmapped=False, in which case unmapped keys will be retained.
    """

    def __init__(self, key_map: dict[str, str], drop_unmapped: bool = True) -> None:
        super().__init__()
        self.key_map = key_map
        self.drop_unmapped = drop_unmapped

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapPackets operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys()
        if tag_keys is None or packet_keys is None:
            return None, None

        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_packet_keys = [
                self.key_map[k] for k in packet_keys if k in self.key_map
            ]
        else:
            mapped_packet_keys = [self.key_map.get(k, k) for k in packet_keys]

        return tag_keys, mapped_packet_keys

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("MapPackets operation requires exactly one stream")

        stream = streams[0]

        def generator():
            for tag, packet in stream:
                if self.drop_unmapped:
                    packet = {
                        v: packet[k] for k, v in self.key_map.items() if k in packet
                    }
                else:
                    packet = {self.key_map.get(k, k): v for k, v in packet.items()}
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        map_repr = ", ".join([f"{k} ⇒ {v}" for k, v in self.key_map.items()])
        return f"packets({map_repr})"

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.key_map,
            self.drop_unmapped,
        ) + tuple(streams)


class DefaultTag(Mapper):
    """
    A Mapper that adds a default tag to the packets in the stream.
    The default tag is added to all packets in the stream. If the
    tag already contains the same key, it will not be overwritten.
    """

    def __init__(self, default_tag: Tag) -> None:
        super().__init__()
        self.default_tag = default_tag

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("DefaultTag operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys()
        tag_keys = list(set(tag_keys or []) | set(self.default_tag.keys()))
        return tag_keys, packet_keys

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("DefaultTag operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in stream:
                yield {**self.default_tag, **tag}, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"DefaultTag({self.default_tag})"


class MapTags(Mapper):
    """
    A Mapper that maps the tags of the packet in the stream to new tags. Packet remains unchanged.
    The mapping is done using a dictionary that maps old tags to new tags.
    If a tag is not in the mapping, it will be dropped from the element unless
    drop_unmapped=False, in which case unmapped tags will be retained.
    """

    def __init__(self, key_map: dict[str, str], drop_unmapped: bool = True) -> None:
        super().__init__()
        self.key_map = key_map
        self.drop_unmapped = drop_unmapped

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapTags operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys()
        if tag_keys is None or packet_keys is None:
            return None, None

        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_tag_keys = [self.key_map[k] for k in tag_keys if k in self.key_map]
        else:
            mapped_tag_keys = [self.key_map.get(k, k) for k in tag_keys]

        return mapped_tag_keys, packet_keys

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("MapTags operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in stream:
                if self.drop_unmapped:
                    tag = {v: tag[k] for k, v in self.key_map.items() if k in tag}
                else:
                    tag = {self.key_map.get(k, k): v for k, v in tag.items()}
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        map_repr = ", ".join([f"{k} ⇒ {v}" for k, v in self.key_map.items()])
        return f"tags({map_repr})"

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.key_map,
            self.drop_unmapped,
        ) + tuple(streams)


class Filter(Mapper):
    """
    A Mapper that filters the packets in the stream based on a predicate function.
    Predicate function should take two arguments: the tag and the packet, both as dictionaries.
    The predicate function should return True for packets that should be kept and False for packets that should be dropped.
    """

    def __init__(self, predicate: Callable[[Tag, Packet], bool]):
        super().__init__()
        self.predicate = predicate

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Filter does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Filter operation requires exactly one stream")

        stream = streams[0]
        return stream.keys()

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("Filter operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in stream:
                if self.predicate(tag, packet):
                    yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"Filter({self.predicate})"

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            function_content_hash(self.predicate),
        ) + tuple(streams)


class Transform(Mapper):
    """
    A Mapper that transforms the packets in the stream based on a transformation function.
    The transformation function should take two arguments: the tag and the packet, both as dictionaries.
    The transformation function should return a tuple of (new_tag, new_packet).
    """

    def __init__(self, transform: Callable[[Tag, Packet], tuple[Tag, Packet]]):
        super().__init__()
        self.transform = transform

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("Transform operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for tag, packet in stream:
                yield self.transform(tag, packet)

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"Transform({self.transform})"

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            hash_function(self.transform),
        ) + tuple(streams)


class Batch(Mapper):
    """
    A Mapper that batches the packets in the stream based on a batch size.
    The batch size is the number of packets to include in each batch.
    If the final batch is smaller than the batch size, it will be dropped unless drop_last=False.
    """

    def __init__(
        self,
        batch_size: int,
        tag_processor: None | Callable[[Collection[Tag]], Tag] = None,
        drop_last: bool = True,
    ):
        super().__init__()
        self.batch_size = batch_size
        if tag_processor is None:
            tag_processor = batch_tags  # noqa: E731

        self.tag_processor = tag_processor
        self.drop_last = drop_last

    def keys(
        self, *streams: SyncStream
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Batch does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Batch operation requires exactly one stream")

        stream = streams[0]
        return stream.keys()

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("Batch operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[tuple[Tag, Packet]]:
            batch_tags: list[Tag] = []
            batch_packets: list[Packet] = []
            for tag, packet in stream:
                batch_tags.append(tag)
                batch_packets.append(packet)
                if len(batch_tags) == self.batch_size:
                    yield self.tag_processor(batch_tags), batch_packet(batch_packets)
                    batch_tags = []
                    batch_packets = []
            if batch_tags and not self.drop_last:
                yield self.tag_processor(batch_tags), batch_packet(batch_packets)

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"Batch(size={self.batch_size}, drop_last={self.drop_last})"

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            self.batch_size,
            hash_function(
                self.tag_processor,
                function_hash_mode="name",
            ),
            self.drop_last,
        ) + tuple(streams)


class GroupBy(Mapper):
    def __init__(
        self,
        group_keys: Collection[str] | None = None,
        reduce_keys: bool = False,
        selection_function: Callable[[Collection[tuple[Tag, Packet]]], Collection[bool]]
        | None = None,
    ) -> None:
        super().__init__()
        self.group_keys = group_keys
        self.reduce_keys = reduce_keys
        self.selection_function = selection_function

    def identity_structure(self, *streams: SyncStream) -> Any:
        struct = (self.__class__.__name__, self.group_keys, self.reduce_keys)
        if self.selection_function is not None:
            struct += (hash_function(self.selection_function),)
        return struct + tuple(streams)

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("GroupBy operation requires exactly one stream")

        stream = streams[0]
        stream_keys, packet_keys = stream.keys()
        stream_keys = stream_keys or []
        packet_keys = packet_keys or []
        group_keys = self.group_keys if self.group_keys is not None else stream_keys

        def generator() -> Iterator[tuple[Tag, Packet]]:
            # step through all packets in the stream and group them by the specified keys
            grouped_packets: dict[tuple, list[tuple[Tag, Packet]]] = defaultdict(list)
            for tag, packet in stream:
                key = tuple(tag.get(key, None) for key in group_keys)
                grouped_packets[key].append((tag, packet))

            for key, packets in grouped_packets.items():
                if self.selection_function is not None:
                    # apply the selection function to the grouped packets
                    selected_packets = self.selection_function(packets)
                    packets = [
                        p for p, selected in zip(packets, selected_packets) if selected
                    ]

                if not packets:
                    continue

                # create a new tag that combines the group keys
                # if reduce_keys is True, we only keep the group keys as a singular value
                new_tag = {}
                if self.reduce_keys:
                    new_tag = {k: key[i] for i, k in enumerate(group_keys)}
                    remaining_keys = set(stream_keys) - set(group_keys)
                else:
                    remaining_keys = set(stream_keys) | set(group_keys)
                # for remaining keys return list of tag values
                for k in remaining_keys:
                    if k not in new_tag:
                        new_tag[k] = [t.get(k, None) for t, _ in packets]
                # combine all packets into a single packet
                combined_packet = {
                    k: [p.get(k, None) for _, p in packets] for k in packet_keys
                }
                yield new_tag, combined_packet

        return SyncStreamFromGenerator(generator)


class CacheStream(Mapper):
    """
    A Mapper that caches the packets in the stream, thus avoiding upstream recomputation.
    The cache is filled the first time the stream is iterated over.
    For the next iterations, the cached packets are returned.
    Call `clear_cache()` to clear the cache.
    """

    def __init__(self) -> None:
        super().__init__()
        self.cache: list[tuple[Tag, Packet]] = []
        self.is_cached = False

    def forward(self, *streams: SyncStream) -> SyncStream:
        if not self.is_cached and len(streams) != 1:
            raise ValueError("CacheStream operation requires exactly one stream")

        def generator() -> Iterator[tuple[Tag, Packet]]:
            if not self.is_cached:
                for tag, packet in streams[0]:
                    self.cache.append((tag, packet))
                    yield tag, packet
                self.is_cached = True
            else:
                for tag, packet in self.cache:
                    yield tag, packet

        return SyncStreamFromGenerator(generator)

    def clear_cache(self) -> None:
        """
        Clear the cache.
        """
        self.cache = []
        self.is_cached = False

    def __repr__(self) -> str:
        return f"CacheStream(active:{self.is_cached})"

    def identity_structure(self, *streams):
        # treat every CacheStream as a different stream
        return None


def tag(
    mapping: dict[str, str], drop_unmapped: bool = True
) -> Callable[[SyncStream], SyncStream]:
    def transformer(stream: SyncStream) -> SyncStream:
        """
        Transform the stream by renaming the keys in the tag.
        The mapping is a dictionary that maps the old keys to the new keys.
        """
        return MapTags(mapping, drop_unmapped=drop_unmapped)(stream)

    return transformer


def packet(
    mapping: dict[str, str], drop_unmapped: bool = True
) -> Callable[[SyncStream], SyncStream]:
    def transformer(stream: SyncStream) -> SyncStream:
        """
        Transform the stream by renaming the keys in the packet.
        The mapping is a dictionary that maps the old keys to the new keys.
        """
        return MapPackets(mapping, drop_unmapped=drop_unmapped)(stream)

    return transformer
