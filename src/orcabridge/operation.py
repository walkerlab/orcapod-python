from .types import Tag, Packet
from typing import Optional, Tuple, List, Dict, Any, Collection, Callable, Iterator
from .hash import hash_dict
import threading

class Operation():
        
    def get_invocation_id(self, *streams: 'SyncStream') -> str:
        # TODO: implement non-order dependent version for order-independent operations
        return ':'.join([str(hash(s)) for s in streams])

    def __call__(self, *streams: 'SyncStream') -> 'SyncStream':
        from .source import Source
        streams = [s() if isinstance(s, Source) else s for s in streams]
        output_stream = self.forward(*streams)
        active_trackers = Tracker.get_active_trackers()

        invocation_id = self.get_invocation_id(*streams)
        for tracker in active_trackers:
            tracker.record(self, invocation_id, [s.source for s in streams])
        output_stream.source = (self, invocation_id)
        return output_stream

    def forward(self, *streams: 'SyncStream') -> 'SyncStream': ...        



from .stream import SyncStream

class Tracker():

    # Thread-local storage to track active trackers
    _local = threading.local()

    def __init__(self) -> None:
        self.active = False
        self.operations: Dict[Tuple[Operation, str], List[Tuple[Operation, str]]] = {}

    def record(self, output_op: Operation, invocation_id: str, upstreams: Collection[Tuple[Operation, str]]) -> None:
        self.operations[(output_op, invocation_id)] = upstreams

    def generate_graph(self):
        pass

    def __enter__(self):
        if not hasattr(self._local, 'active_trackers'):
            self._local.active_trackers = []

        self._local.active_trackers.append(self)
        self.active = True
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        # Remove this tracker from active trackers
        if hasattr(self._local, 'active_trackers'):
            self._local.active_trackers.remove(self)
        self.active = False
        

    @classmethod
    def get_active_trackers(cls) -> List['Tracker']:
        if hasattr(cls._local, 'active_trackers'):
            return cls._local.active_trackers
        return []
    


