# Collection of base classes for operations and streams in the orcabridge framework.
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Iterator
from typing import Any

from orcabridge.hashing import HashableMixin
from orcabridge.types import Packet, Tag


class Operation(ABC, HashableMixin):
    """
    Operation defines a generic operation that can be performed on a stream of data.
    It is a base class for all operations that can be performed on a collection of streams
    (including an empty collection).
    The operation is defined as a callable that takes a collection of streams as input
    and returns a new stream as output.
    Each invocation of the operation is assigned a unique ID. The corresponding invocation
    information is stored as Invocation object and attached to the output stream.
    """

    def __init__(self, label: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._label = label

    def keys(
        self, *streams: "SyncStream"
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the operation.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned if it is feasible to do so, otherwise a tuple
        (None, None) is returned to signify that the keys are not known.
        """
        return None, None

    @property
    def label(self) -> str:
        """
        Returns a human-readable label for this operation.
        Default implementation returns the provided label or class name if no label was provided.
        """
        if self._label:
            return self._label
        return self.__class__.__name__

    @label.setter
    def label(self, value: str) -> None:
        self._label = value

    def identity_structure(self, *streams: "SyncStream") -> Any:
        # Default implementation of identity_structure for the operation only
        # concerns the operation class and the streams if present. Subclasses of
        # Operations should override this method to provide a more meaningful
        # representation of the operation.
        return (self.__class__.__name__,) + tuple(streams)

    def __call__(self, *streams: "SyncStream", **kwargs) -> "SyncStream":
        # trigger call on source if passed as stream

        normalized_streams = [
            stream() if isinstance(stream, Source) else stream for stream in streams
        ]
        output_stream = self.forward(*normalized_streams, **kwargs)
        # create an invocation instance
        invocation = Invocation(self, normalized_streams)
        # label the output_stream with the invocation information
        output_stream.invocation = invocation

        # register the invocation with active trackers
        active_trackers = Tracker.get_active_trackers()
        for tracker in active_trackers:
            tracker.record(invocation)

        return output_stream

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        if self._label is not None:
            return f"{self.__class__.__name__}({self._label})"
        return self.__class__.__name__

    @abstractmethod
    def forward(self, *streams: "SyncStream") -> "SyncStream": ...


class Tracker(ABC):
    """
    A tracker is a class that can track the invocations of operations. Only "active" trackers
    participate in tracking and its `record` method gets called on each invocation of an operation.
    Multiple trackers can be active at any time.
    """

    _local = threading.local()

    @classmethod
    def get_active_trackers(cls) -> list["Tracker"]:
        if hasattr(cls._local, "active_trackers"):
            return cls._local.active_trackers
        return []

    def __init__(self):
        self.active = False

    def activate(self) -> None:
        """
        Activate the tracker. This is a no-op if the tracker is already active.
        """
        if not self.active:
            if not hasattr(self._local, "active_trackers"):
                self._local.active_trackers = []
            self._local.active_trackers.append(self)
            self.active = True

    def deactivate(self) -> None:
        # Remove this tracker from active trackers
        if hasattr(self._local, "active_trackers") and self.active:
            if self in self._local.active_trackers:
                self._local.active_trackers.remove(self)
            self.active = False

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        self.deactivate()

    @abstractmethod
    def record(self, invocation: "Invocation") -> None: ...


# This is NOT an abstract class, but rather a concrete class that
# represents an invocation of an operation on a collection of streams.
class Invocation(HashableMixin):
    """
    This class represents an invocation of an operation on a collection of streams.
    It contains the operation and the streams that were used in the invocation.
    Note that the collection of streams may be empty, in which case the invocation
    likely corresponds to a source operation.
    """

    def __init__(
        self,
        operation: Operation,
        streams: Collection["SyncStream"],
    ) -> None:
        self.operation = operation
        self.streams = streams

    def __hash__(self) -> int:
        return super().__hash__()

    def __repr__(self) -> str:
        return f"Invocation({self.operation}, ID:{hash(self)})"

    def keys(self) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.operation.keys(*self.streams)

    def identity_structure(self) -> int:
        # Identity of an invocation is entirely dependend on
        # the operation's identity structure upon invocation
        return self.operation.identity_structure(*self.streams)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Invocation):
            return False
        return hash(self) == hash(other)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Invocation):
            return NotImplemented

        if self.operation == other.operation:
            return hash(self) < hash(other)
        # otherwise, order by the operation
        return hash(self.operation) < hash(other.operation)


class Stream(ABC, HashableMixin):
    """
    A stream is a collection of tagged-packets that are generated by an operation.
    The stream is iterable and can be used to access the packets in the stream.

    A stream has propery `invocation` that is an instance of Invocation that generated the stream.
    This may be None if the stream is not generated by an operation.
    """

    def __init__(self, label: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._invocation: Invocation | None = None
        self._label = label

    def identity_structure(self) -> Any:
        """
        Identity structure of a stream is deferred to the identity structure
        of the associated invocation, if present.
        A bare stream without invocation has no well-defined identity structure.
        """
        if self.invocation is not None:
            return self.invocation.identity_structure()
        return super().identity_structure()

    @property
    def label(self) -> str:
        """
        Returns a human-readable label for this stream.
        If no label is provided and the stream is generated by an operation,
        the label of the operation is used.
        Otherwise, the class name is used as the label.
        """
        if self._label is None:
            if self.invocation is not None:
                # use the invocation operation label
                return self.invocation.operation.label
            else:
                return self.__class__.__name__
        return self._label

    @property
    def invocation(self) -> Invocation | None:
        return self._invocation

    @invocation.setter
    def invocation(self, value: Invocation) -> None:
        if not isinstance(value, Invocation):
            raise TypeError("invocation field must be an instance of Invocation")
        self._invocation = value

    def keys(self) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the stream.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned on based-effort basis, and this invocation may trigger the
        upstream computation of the stream.
        Furthermore, the keys are not guaranteed to be identical across all packets in the stream.
        This method is useful for inferring the keys of the stream without having to iterate
        over the entire stream.
        """
        if self.invocation is not None:
            # if the stream is generated by an operation, use the keys from the invocation
            tag_keys, packet_keys = self.invocation.keys()
            if tag_keys is not None and packet_keys is not None:
                return tag_keys, packet_keys
        # otherwise, use the keys from the first packet in the stream
        # note that this may be computationally expensive
        tag, packet = next(iter(self))
        return list(tag.keys()), list(packet.keys())

    @abstractmethod
    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        raise NotImplementedError("Subclasses must implement __iter__ method")

    def flow(self) -> Collection[tuple[Tag, Packet]]:
        """
        Flow everything through the stream, returning the entire collection of
        (Tag, Packet) as a collection. This will tigger any upstream computation of the stream.
        """
        return list(self)


class SyncStream(Stream):
    """
    A stream that will complete in a fixed amount of time.
    It is suitable for synchronous operations that
    will have to wait for the stream to finish before proceeding.
    """

    def head(self, n: int = 5) -> None:
        """
        Print the first n elements of the stream.
        This method is useful for previewing the stream
        without having to iterate over the entire stream.
        If n is <= 0, the entire stream is printed.
        """
        for idx, (tag, packet) in enumerate(self):
            if n > 0 and idx >= n:
                break
            print(f"Tag: {tag}, Packet: {packet}")

    def __len__(self) -> int:
        """
        Returns the number of packets in the stream.
        Note that this method may trigger the upstream computation of the stream.
        This method is not guaranteed to be efficient and should be used with caution.
        """
        return sum(1 for _ in self)

    def __rshift__(
        self, transformer: Callable[["SyncStream"], "SyncStream"]
    ) -> "SyncStream":
        """
        Returns a new stream that is the result of applying the mapping to the stream.
        The mapping is applied to each packet in the stream and the resulting packets
        are returned in a new stream.
        """
        # TODO: remove just in time import
        from .mapper import MapPackets

        if isinstance(transformer, dict):
            return MapPackets(transformer)(self)
        elif isinstance(transformer, Callable):
            return transformer(self)

    def __mul__(self, other: "SyncStream") -> "SyncStream":
        """
        Returns a new stream that is the result joining with the other stream
        """
        # TODO: remove just in time import
        from .mapper import Join

        if not isinstance(other, SyncStream):
            raise TypeError("other must be a SyncStream")
        return Join()(self, other)


class Mapper(Operation):
    """
    A Mapper is an operation that does NOT generate new file content.
    It is used to control the flow of data in the pipeline without modifying or creating new data (file).
    """


class Source(Operation, SyncStream):
    """
    A base class for all sources in the system. A source can be seen as a special
    type of Operation that takes no input and produces a stream of packets.
    For convenience, the source itself is also a stream and thus can be used
    as an input to other operations directly.
    """

    def __init__(self, label: str | None = None, **kwargs) -> None:
        super().__init__(label=label, **kwargs)
        self._invocation = None

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        yield from self()
