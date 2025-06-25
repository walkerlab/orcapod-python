from collections import defaultdict
from collections.abc import Callable, Collection, Iterator
from itertools import chain
from typing import Any

from orcapod.types import Packet, Tag, TypeSpec
from orcapod.hashing import function_content_hash, hash_function
from orcapod.core.base import Kernel, SyncStream, Operator
from orcapod.core.streams import SyncStreamFromGenerator
from orcapod.utils.stream_utils import (
    batch_packet,
    batch_tags,
    check_packet_compatibility,
    intersection_typespecs,
    join_tags,
    semijoin_tags,
    union_typespecs,
    intersection_typespecs,
    fill_missing
)



class Repeat(Operator):
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

    def identity_structure(self, *streams) -> tuple[str, int, set[SyncStream]]:
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, self.repeat_count, set(streams))

    def keys(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Repeat does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Repeat operation requires exactly one stream")

        stream = streams[0]
        return stream.keys(trigger_run=trigger_run)

    def types(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Repeat does not alter the types of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Repeat operation requires exactly one stream")

        stream = streams[0]
        return stream.types(trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> bool | None:
        if len(streams) != 1:
            raise ValueError(
                "Repeat operation only supports operating on a single input stream"
            )

        # Repeat's uniquness is true only if (1) input stream has unique tags and (2) repeat count is 1
        return self.repeat_count == 1 and streams[0].claims_unique_tags(
            trigger_run=trigger_run
        )


class Merge(Operator):
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

    def identity_structure(self, *streams):
        # Merge does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Merge does not alter the keys of the stream.
        """
        if len(streams) < 2:
            raise ValueError("Merge operation requires at least two streams")

        merged_tag_keys = set()
        merged_packet_keys = set()

        for stream in streams:
            tag_keys, packet_keys = stream.keys(trigger_run=trigger_run)
            if tag_keys is not None:
                merged_tag_keys.update(set(tag_keys))
            if packet_keys is not None:
                merged_packet_keys.update(set(packet_keys))

        return list(merged_tag_keys), list(merged_packet_keys)

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Merge does not alter the types of the stream.
        """
        if len(streams) < 2:
            raise ValueError("Merge operation requires at least two streams")

        merged_tag_types: TypeSpec | None = {}
        merged_packet_types: TypeSpec | None = {}

        for stream in streams:
            if merged_tag_types is None and merged_packet_types is None:
                break
            tag_types, packet_types = stream.types(trigger_run=trigger_run)
            if merged_tag_types is not None and tag_types is not None:
                merged_tag_types.update(tag_types)
            else:
                merged_tag_types = None
            if merged_tag_types is not None and packet_types is not None:
                merged_packet_types.update(packet_types)
            else:
                merged_tag_types = None

        return merged_tag_types, merged_packet_types

    def claims_unique_tags(
        self, *streams: SyncStream, trigger_run: bool = True
    ) -> bool | None:
        """
        Merge operation can only claim unique tags if all input streams have unique tags AND
        the tag keys are not identical across all streams.
        """
        # TODO: update implementation
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

def union_lists(left, right):
    if left is None or right is None:
        return None
    output = list(left)
    for item in right:
        if item not in output:
            output.append(item)
    return output
                  

class Join(Operator):
    def identity_structure(self, *streams):
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the types of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned if it is feasible to do so, otherwise a tuple
        (None, None) is returned to signify that the keys are not known.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_keys, left_packet_keys = left_stream.keys(trigger_run=trigger_run)
        right_tag_keys, right_packet_keys = right_stream.keys(trigger_run=trigger_run)

        # TODO: do error handling when merge fails
        joined_tag_keys = union_lists(left_tag_keys, right_tag_keys)
        joined_packet_keys = union_lists(left_packet_keys, right_packet_keys)

        return joined_tag_keys, joined_packet_keys

    def types(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Returns the types of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned if it is feasible to do so, otherwise a tuple
        (None, None) is returned to signify that the keys are not known.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_types, left_packet_types = left_stream.types(trigger_run=False)
        right_tag_types, right_packet_types = right_stream.types(trigger_run=False)

        # TODO: do error handling when merge fails
        joined_tag_types = union_typespecs(left_tag_types, right_tag_types)
        joined_packet_types = union_typespecs(left_packet_types, right_packet_types)

        return joined_tag_types, joined_packet_types

    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams

        def generator() -> Iterator[tuple[Tag, Packet]]:
            # using list comprehension rather than list() to avoid call to __len__ which is expensive
            left_stream_buffered = [e for e in left_stream]
            right_stream_buffered = [e for e in right_stream]
            for left_tag, left_packet in left_stream_buffered:
                for right_tag, right_packet in right_stream_buffered:
                    if (joined_tag := join_tags(left_tag, right_tag)) is not None:
                        if not check_packet_compatibility(left_packet, right_packet):
                            raise ValueError(
                                f"Packets are not compatible: {left_packet} and {right_packet}"
                            )
                        yield joined_tag, {**left_packet, **right_packet}

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "Join()"


class FirstMatch(Operator):
    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags, returning at most one match for each tag.
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

    def identity_structure(self, *streams: SyncStream) -> tuple[str, set[SyncStream]]:
        # Join does not depend on the order of the streams -- convert it onto a set
        return (self.__class__.__name__, set(streams))

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
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
        left_tag_keys, left_packet_keys = left_stream.keys(trigger_run=trigger_run)
        right_tag_keys, right_packet_keys = right_stream.keys(trigger_run=trigger_run)

        # if any of the components return None -> resolve to default operation
        if (
            left_tag_keys is None
            or right_tag_keys is None
            or left_packet_keys is None
            or right_packet_keys is None
        ):
            return super().keys(*streams, trigger_run=trigger_run)

        joined_tag_keys = list(set(left_tag_keys) | set(right_tag_keys))
        joined_packet_keys = list(set(left_packet_keys) | set(right_packet_keys))

        return joined_tag_keys, joined_packet_keys

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Returns the typespecs of tag and packet.
        """
        if len(streams) != 2:
            raise ValueError("FirstMatch operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_types, left_packet_types = left_stream.types(trigger_run=trigger_run)
        right_tag_types, right_packet_types = right_stream.types(
            trigger_run=trigger_run
        )

        # if any of the components return None -> resolve to default operation
        if (
            left_tag_types is None
            or right_tag_types is None
            or left_packet_types is None
            or right_packet_types is None
        ):
            return super().types(*streams, trigger_run=trigger_run)

        joined_tag_types = union_typespecs(left_tag_types, right_tag_types)
        joined_packet_types = union_typespecs(left_packet_types, right_packet_types)

        return joined_tag_types, joined_packet_types


class MapPackets(Operator):
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

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapPackets operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys(trigger_run=trigger_run)
        if tag_keys is None or packet_keys is None:
            super_tag_keys, super_packet_keys =  super().keys(trigger_run=trigger_run)
            tag_keys = tag_keys or super_tag_keys
            packet_keys = packet_keys or super_packet_keys

        if packet_keys is None:
            return tag_keys, packet_keys

        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_packet_keys = [
                self.key_map[k] for k in packet_keys if k in self.key_map
            ]
        else:
            mapped_packet_keys = [self.key_map.get(k, k) for k in packet_keys]

        return tag_keys, mapped_packet_keys

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Returns the types of the operation.
        The first list contains the types of the tags, and the second list contains the types of the packets.
        The types are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapPackets operation requires exactly one stream")

        stream = streams[0]
        tag_types, packet_types = stream.types(trigger_run=trigger_run)
        if tag_types is None or packet_types is None:
            super_tag_types, super_packet_types = super().types(trigger_run=trigger_run)
            tag_types = tag_types or super_tag_types
            packet_types = packet_types or super_packet_types

        if packet_types is None:
            return tag_types, packet_types

        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_packet_types = {
                self.key_map[k]: v for k, v in packet_types.items() if k in self.key_map
            }
        else:
            mapped_packet_types = {
                self.key_map.get(k, k): v for k, v in packet_types.items()
            }

        return tag_types, mapped_packet_types


class DefaultTag(Operator):
    """
    A Mapper that adds a default tag to the packets in the stream.
    The default tag is added to all packets in the stream. If the
    tag already contains the same key, it will not be overwritten.
    """

    def __init__(self, default_tag: Tag) -> None:
        super().__init__()
        self.default_tag = default_tag

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

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("DefaultTag operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys(trigger_run=trigger_run)
        if tag_keys is None or packet_keys is None:
            return super().keys(trigger_run=trigger_run)
        tag_keys = list(set(tag_keys) | set(self.default_tag.keys()))
        return tag_keys, packet_keys


class MapTags(Operator):
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

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are inferred based on the first (tag, packet) pair in the stream.
        """
        if len(streams) != 1:
            raise ValueError("MapTags operation requires exactly one stream")

        stream = streams[0]
        tag_keys, packet_keys = stream.keys(trigger_run=trigger_run)
        if tag_keys is None or packet_keys is None:
            return super().keys(trigger_run=trigger_run)

        if self.drop_unmapped:
            # If drop_unmapped is True, we only keep the keys that are in the mapping
            mapped_tag_keys = [self.key_map[k] for k in tag_keys if k in self.key_map]
        else:
            mapped_tag_keys = [self.key_map.get(k, k) for k in tag_keys]

        return mapped_tag_keys, packet_keys

class SemiJoin(Operator):
    """
    Perform semi-join on the left stream tags with the tags of the right stream
    """
    def identity_structure(self, *streams):
        # Restrict DOES depend on the order of the streams -- maintain as a tuple
        return (self.__class__.__name__,) + streams

    def keys(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        For semijoin, output keys and types are identical to left stream
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        return streams[0].keys(trigger_run=trigger_run)

    def types(
        self, *streams: SyncStream, trigger_run=False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        For semijoin, output keys and types are identical to left stream
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        return streams[0].types(trigger_run=trigger_run)

    def forward(self, *streams: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) != 2:
            raise ValueError("Join operation requires exactly two streams")

        left_stream, right_stream = streams
        left_tag_typespec, left_packet_typespec = left_stream.types()
        right_tag_typespec, right_packet_typespec = right_stream.types()

        common_tag_typespec = intersection_typespecs(left_tag_typespec, right_tag_typespec)
        common_tag_keys = None
        if common_tag_typespec is not None:
            common_tag_keys = list(common_tag_typespec.keys())

        def generator() -> Iterator[tuple[Tag, Packet]]:
            # using list comprehension rather than list() to avoid call to __len__ which is expensive
            left_stream_buffered = [e for e in left_stream]
            right_stream_buffered = [e for e in right_stream]
            for left_tag, left_packet in left_stream_buffered:
                for right_tag, _ in right_stream_buffered:
                    if semijoin_tags(left_tag, right_tag, common_tag_keys) is not None:
                        yield left_tag, left_packet
                        # move onto next entry
                        break

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return "SemiJoin()"

class Filter(Operator):
    """
    A Mapper that filters the packets in the stream based on a predicate function.
    Predicate function should take two arguments: the tag and the packet, both as dictionaries.
    The predicate function should return True for packets that should be kept and False for packets that should be dropped.
    """

    def __init__(self, predicate: Callable[[Tag, Packet], bool]):
        super().__init__()
        self.predicate = predicate

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

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Filter does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Filter operation requires exactly one stream")

        stream = streams[0]
        return stream.keys(trigger_run=trigger_run)


class Transform(Operator):
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


class Batch(Operator):
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

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Batch does not alter the keys of the stream.
        """
        if len(streams) != 1:
            raise ValueError("Batch operation requires exactly one stream")

        stream = streams[0]
        return stream.keys(trigger_run=trigger_run)


class GroupBy(Operator):
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
                new_tag: Tag = {}
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
                combined_packet: Packet = {
                    k: [p.get(k, None) for _, p in packets] for k in packet_keys
                }
                yield new_tag, combined_packet

        return SyncStreamFromGenerator(generator)

    def identity_structure(self, *streams: SyncStream) -> Any:
        struct = (self.__class__.__name__, self.group_keys, self.reduce_keys)
        if self.selection_function is not None:
            struct += (hash_function(self.selection_function),)
        return struct + tuple(streams)


class CacheStream(Operator):
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
