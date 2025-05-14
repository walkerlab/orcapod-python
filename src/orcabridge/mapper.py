from typing import Callable, Dict, Optional, List, Sequence

from .stream import SyncStream, SyncStreamFromGenerator
from .base import Operation
from .utils.stream_utils import (
    join_tags,
    check_packet_compatibility,
    batch_tag,
    batch_packet,
)
from .hashing import hash_function
from .types import Tag, Packet
from typing import Iterator, Tuple, Any, Collection


class Mapper(Operation):
    """
    A Mapper is an operation that does NOT generate new file content.
    It is used to control the flow of data in the pipeline without modifying or creating new data (file).
    """

class Repeat(Mapper):
    """
    A Mapper that repeats the packets in the stream a specified number of times.
    The repeat count is the number of times to repeat each packet.
    """

    def __init__(self, repeat_count: int) -> None:
        super().__init__()
        self.repeat_count = repeat_count

    def identity_structure(self, *streams):
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, self.repeat_count, set(streams))

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
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

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            for tag, packet in stream:
                for _ in range(self.repeat_count):
                    yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"Repeat(count={self.repeat_count})"


class Join(Mapper):
    def identity_structure(self, *streams):
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
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

        joined_tag_keys = list(set(left_tag_keys) | set(right_tag_keys))
        joined_packet_keys = list(set(left_packet_keys) | set(right_packet_keys))

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
                            raise ValueError(f"Packets are not compatible: {left_packet} and {right_packet}")
                        yield joined_tag, {**left_packet, **right_packet}

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "Join()"


class FirstMatch(Mapper):
    
    def identity_structure(self, *streams):
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
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

        joined_tag_keys = list(set(left_tag_keys) | set(right_tag_keys))
        joined_packet_keys = list(set(left_packet_keys) | set(right_packet_keys))

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
                            raise ValueError(f"Packets are not compatible: {outer_packet} and {inner_packet}")
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

    def __init__(self, key_map: Dict[str, str], drop_unmapped: bool = True) -> None:
        super().__init__()
        self.key_map = key_map
        self.drop_unmapped = drop_unmapped

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapPackets operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys()
        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_packet_keys = [self.key_map[k] for k in packet_keys if k in self.key_map]
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
                    packet = {v: packet[k] for k, v in self.key_map.items() if k in packet}
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


class MapTags(Mapper):
    """
    A Mapper that maps the tags of the packet in the stream to new tags. Packet remains unchanged.
    The mapping is done using a dictionary that maps old tags to new tags.
    If a tag is not in the mapping, it will be dropped from the element unless
    drop_unmapped=False, in which case unmapped tags will be retained.
    """

    def __init__(self, key_map: Dict[str, str], drop_unmapped: bool = True) -> None:
        super().__init__()
        self.key_map = key_map
        self.drop_unmapped = drop_unmapped

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapTags operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys()
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

        def generator() -> Iterator[Tuple[Tag, Packet]]:
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

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
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

        def generator() -> Iterator[Tuple[Tag, Packet]]:
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

    def __init__(self, transform: Callable[[Tag, Packet], Tuple[Tag, Packet]]):
        super().__init__()
        self.transform = transform

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("Transform operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[Tuple[Tag, Packet]]:
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
        tag_processor: Optional[Callable[[Sequence[Tag]], Tag]] = None,
        drop_last: bool = True,
    ):
        super().__init__()
        self.batch_size = batch_size
        if tag_processor is None:
            tag_processor = lambda tags: batch_tag(tags)

        self.tag_processor = tag_processor
        self.drop_last = drop_last

    def keys(self, *streams: SyncStream) -> Tuple[Collection[str], Collection[str]]:
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

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            batch_tags: List[Tag] = []
            batch_packets: List[Packet] = []
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


class CacheStream(Mapper):
    """
    A Mapper that caches the packets in the stream, thus avoiding upstream recomputation.
    The cache is filled the first time the stream is iterated over.
    For the next iterations, the cached packets are returned.
    Call `clear_cache()` to clear the cache.
    """

    def __init__(self) -> None:
        super().__init__()
        self.cache: List[Tuple[Tag, Packet]] = []
        self.is_cached = False

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 1:
            raise ValueError("CacheStream operation requires exactly one stream")

        stream = streams[0]

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            if not self.is_cached:
                for tag, packet in stream:
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


def tag(mapping: Dict[str, str], drop_unmapped: bool = True) -> Callable[[SyncStream], SyncStream]:
    def transformer(stream: SyncStream) -> SyncStream:
        """
        Transform the stream by renaming the keys in the tag.
        The mapping is a dictionary that maps the old keys to the new keys.
        """
        return MapTags(mapping, drop_unmapped=drop_unmapped)(stream)

    return transformer


def packet(mapping: Dict[str, str], drop_unmapped: bool = True) -> Callable[[SyncStream], SyncStream]:
    def transformer(stream: SyncStream) -> SyncStream:
        """
        Transform the stream by renaming the keys in the packet.
        The mapping is a dictionary that maps the old keys to the new keys.
        """
        return MapPackets(mapping, drop_unmapped=drop_unmapped)(stream)

    return transformer
