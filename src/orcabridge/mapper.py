
from typing import Callable

from .stream import Stream, SyncStream
from .operation import Operation
from .tag import join_tags, batch_tags



class Mapper(Operation):
    """
    A Mapper is an operation that does NOT generate new file content.
    It is used to control the flow of data in the pipeline without modifying or creating new data (file).
    """
   

class Join(Mapper):
    def __call__(self, left_stream: SyncStream, right_stream: SyncStream) -> SyncStream:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        # TODO: warn if packet contains the same key
        def generator():
            for left_tag, left_packet in left_stream:
                for right_tag, right_packet in right_stream:
                    if (joined_tag := join_tags(left_tag, right_tag)) is not None:
                        yield joined_tag, {**left_packet, **right_packet}
        
        return SyncStream(generator)

class MapKeys(Mapper):
    """
    A Mapper that maps the keys of the packet in the stream to new keys.
    The mapping is done using a dictionary that maps old keys to new keys.
    If a key is not in the mapping, it will be dropped from the element unless
    drop_unmapped=False, in which case unmapped keys will be retained.
    """
    def __init__(self, key_map: dict, drop_unmapped: bool = True):
        self.key_map = key_map
        self.drop_unmapped = drop_unmapped

    def __call__(self, stream: Stream) -> Stream:
        def generator():
            for tag, packet in stream:
                if self.drop_unmapped:
                    packet = {v: packet[k] for k, v in self.key_map.items() if k in packet}
                else:
                    packet = {self.key_map.get(k, k): v for k, v in packet.items()}
                yield tag, packet
        return SyncStream(generator)


class MapTags(Operation):
    """
    A Mapper that maps the tags of the packet in the stream to new tags. Packet remains unchanged.
    The mapping is done using a dictionary that maps old tags to new tags.
    If a tag is not in the mapping, it will be dropped from the element unless
    drop_unmapped=False, in which case unmapped tags will be retained.
    """
    def __init__(self, tag_map, drop_unmapped=True):
        self.tag_map = tag_map
        self.drop_unmapped = drop_unmapped

    def __call__(self, stream: Stream) -> Stream:
        def generator():
            for tag, packet in stream:
                if self.drop_unmapped:
                    tag = {v: tag[k] for k, v in self.tag_map.items() if k in tag}
                else:
                    tag = {self.tag_map.get(k, k): v for k, v in tag.items()}
                yield tag, packet
        return SyncStream(generator)
        
class Filter(Mapper):
    """
    A Mapper that filters the packets in the stream based on a predicate function.
    Predicate function should take two arguments: the tag and the packet, both as dictionaries.
    The predicate function should return True for packets that should be kept and False for packets that should be dropped.
    """
    def __init__(self, predicate: Callable[[dict, dict], bool]):
        self.predicate = predicate

    def __call__(self, stream: Stream) -> Stream:
        def generator():
            for tag, packet in stream:
                if self.predicate(tag, packet):
                    yield tag, packet
        return SyncStream(generator)
    
class Batch(Mapper):
    """
    A Mapper that batches the packets in the stream based on a batch size.
    The batch size is the number of packets to include in each batch.
    If the final batch is smaller than the batch size, it will be dropped unless drop_last=False.
    """
    def __init__(self, batch_size: int, tag_processor: Callable[[list[dict]], dict] = None, drop_last: bool = True):
        self.batch_size = batch_size
        if tag_processor is None:
            tag_processor = lambda tags: batch_tags(tags)
        self.tag_processor = tag_processor
        self.drop_last = drop_last

    def __call__(self, stream: Stream) -> Stream:
        def generator():
            batch_tags = []
            batch_packets = []
            for tag, packet in stream:
                batch_tags.append(tag)
                batch_packets.append(packet)
                if len(batch_tags) == self.batch_size:
                    yield self.tag_processor(batch_tags), batch_packets
                    batch_tags = []
                    batch_packets = []
            if batch_tags and not self.drop_last:
                yield self.tag_processor(batch_tags), batch_packets

        return SyncStream(generator)