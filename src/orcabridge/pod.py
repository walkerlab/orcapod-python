import logging
logger = logging.getLogger(__name__)

from .operation import Operation
from .mapper import Join
from .stream import SyncStream

class FunctionPod(Operation):
    def __init__(self, function, output_keys=None):
        self.function = function
        self.output_keys = output_keys

    def __call__(self, *streams: SyncStream) -> SyncStream:
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join(stream, next_stream)
        else:
            stream = streams[0]

        def generator():
            n_computed = 0
            for tag, packet in stream:
                memoized, output_packet = self.memoize(packet)
                if memoized:
                    yield tag, output_packet
                    continue
                values = self.function(**packet)
                if len(self.output_keys) == 1:
                    values = [values]
                if len(values) != len(self.output_keys):
                    raise ValueError("Number of output keys does not match number of values returned by function")
                
                output_packet = {k: v for k, v in zip(self.output_keys, values)}

                status = self.store(tag, packet, output_packet)
                logger.info(f"Store status for element {packet}: {status}")

                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet
        return SyncStream(generator)
        
    def store(self, tag, packet, output_packet):
        return False
    
    def memoize(self, packet):
        # TODO: consider just using None to indicate no memoization
        return False, None
