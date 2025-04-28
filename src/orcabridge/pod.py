import logging
logger = logging.getLogger(__name__)

from typing import List, Optional, Tuple, Iterator, Iterable
from .operation import Operation
from .mapper import Join
from .stream import SyncStream, SyncStreamFromGenerator
from .types import Tag, Packet, PodFunction


class Pod(Operation):
    pass


class FunctionPod(Pod):
    def __init__(self, function: PodFunction, output_keys: Optional[List[str]] = None) -> None:
        self.function = function
        if output_keys is None:
            output_keys = []
        self.output_keys = output_keys

    def __call__(self, *streams: SyncStream) -> SyncStream:
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
                memoized_packet = self.memoize(packet)
                if memoized_packet is not None:
                    yield tag, memoized_packet
                    continue
                values = self.function(**packet)
                if len(self.output_keys) == 1:
                    values = [values]
                elif isinstance(values, Iterable):
                    values = list(values)
                else:
                    raise ValueError("Values returned by function must be a pathlike or a sequence of pathlikes")
                
                if len(values) != len(self.output_keys):
                    raise ValueError("Number of output keys does not match number of values returned by function")
                
                output_packet: Packet = {k: v for k, v in zip(self.output_keys, values)}

                status = self.store(tag, packet, output_packet)
                logger.info(f"Store status for element {packet}: {status}")

                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet
        return SyncStreamFromGenerator(generator)
        
    def store(self, tag: Tag, packet: Packet, output_packet: Packet) -> bool:
        return False
    
    def memoize(self, packet: Packet) -> Optional[Packet]:
        return None
