from .types import Tag, Packet
from typing import Optional, Tuple, List, Dict, Any, Collection, Callable, Iterator
from .hash import hash_dict
import networkx as nx
import threading


class Operation():
        
    def get_invocation(self, *streams: 'SyncStream') -> str:
        # default implementation where ID is stream order sensitive
        invocation_id =  ':'.join([str(hash(s)) for s in streams])
        return Invocation(self, invocation_id, streams)

    def __call__(self, *streams: 'SyncStream') -> 'SyncStream':
        # if any source is passed in as a stream, invoke it to extract stream first
        from .source import Source
        streams = [s() if isinstance(s, Source) else s for s in streams]

        output_stream = self.forward(*streams)
        # create an invocation instance
        invocation = self.get_invocation(*streams)
        # label the output_stream with the invocation information
        output_stream.source = invocation
        
        # reg
        active_trackers = Tracker.get_active_trackers()
        for tracker in active_trackers:
            tracker.record(invocation)
        
        return output_stream
    
    def __repr__(self):
        return self.__class__.__name__

    def forward(self, *streams: 'SyncStream') -> 'SyncStream': ...        

class Invocation():
    def __init__(self, operation: Operation, invocation_id: str, streams: Collection['SyncStream']) -> None:
        self.operation = operation
        self.invocation_id = invocation_id
        self.streams = streams

    def __repr__(self) -> str:
        return f"{self.operation}(ID:{self.invocation_id})"

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Invocation):
            return False
        return self.operation == other.operation and self.invocation_id == other.invocation_id

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Invocation):
            return NotImplemented

        if self.operation == other.operation:
            return self.invocation_id < other.invocation_id
        return self.operation < other.operation
    
from .stream import SyncStream

class Tracker():

    # Thread-local storage to track active trackers
    _local = threading.local()

    def __init__(self) -> None:
        self.active = False
        self.invocation_lut: Dict[Operation, Collection[Invocation]] = {}

    def record(self, invocation: Invocation) -> None:
        self.invocation_lut.setdefault(invocation.operation, set()).add(invocation)

    def reset(self) -> Dict[Operation, Collection[Invocation]]:
        """
        Reset the tracker and return the recorded invocations.
        """
        recorded_invocations = self.invocation_lut
        self.invocation_lut = {}
        return recorded_invocations
        

    def generate_namemap(self) -> Dict[Invocation, str]:
        namemap = {}
        for operation, invocations in self.invocation_lut.items():
            # if only one entry present, use the operation name alone
            invocations = sorted(invocations)
            if len(invocations) == 1:
                namemap[invocations[0]] = f"{operation}"
                continue
            # if multiple entries, use the operation name and index
            for idx, invocation in enumerate(invocations):
                namemap[invocation] = f"{operation}_{idx}"
        return namemap
    
    def activate(self) -> None:
        """
        Activate the tracker. This is a no-op if the tracker is already active.
        """
        if not self.active:
            if not hasattr(self._local, 'active_trackers'):
                self._local.active_trackers = []
            self._local.active_trackers.append(self)
            self.active = True

    def deactivate(self) -> None:
        # Remove this tracker from active trackers
        if hasattr(self._local, 'active_trackers') and self.active:
            self._local.active_trackers.remove(self)
            self.active = False
        

    def generate_graph(self):
        G = nx.DiGraph()

        # Add edges for each invocation
        for operation, invocations in self.invocation_lut.items():
            for invocation in invocations:
                for upstream in invocation.streams:
                    # if upstream.source is not in the graph, add it
                    if upstream.source not in G:
                        G.add_node(upstream.source)
                    G.add_edge(upstream.source, invocation)
        
        return G


    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.deactivate()
        

    @classmethod
    def get_active_trackers(cls) -> List['Tracker']:
        if hasattr(cls._local, 'active_trackers'):
            return cls._local.active_trackers
        return []
    


