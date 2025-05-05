from .types import Tag, Packet
from typing import (
    Optional,
    Tuple,
    List,
    Dict,
    Any,
    Collection,
    Callable,
    Iterator,
    Mapping,
    Union,
)
from .utils.hash import hash_dict, stable_hash
import hashlib
import networkx as nx
import json
from collections.abc import Mapping, Collection
from typing import Any, Dict, List, Set, Tuple, Union


class HashableMixin:
    """A mixin that provides content-based hashing functionality."""

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.
        By default, returns None to indicate that no custom structure is provided.
        Subclasses should override this method to provide meaningful representations.

        Returns:
            None to indicate no custom structure (use default hash)
        """
        return None

    def content_hash(self, char_count: Optional[int] = 16) -> str:
        """
        Generate a stable string hash based on the object's content.

        Returns:
            str: A hexadecimal digest representing the object's content
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the superclass's hash
        if structure is None:
            # Convert the default hash to a stable string
            return hashlib.sha256(str(super().__hash__()).encode()).hexdigest()

        # Generate a hash from the identity structure
        return self._hash_structure(structure, char_count=char_count)

    def content_hash_int(self, hexdigits=16) -> int:
        """
        Generate a stable integer hash based on the object's content.

        Returns:
            int: An integer representing the object's content
        """
        # pass in char_count=None to get the full hash
        return int(self.content_hash(char_count=None)[:hexdigits], 16)

    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the superclass's hash method.

        Returns:
            int: A hash value based on either content or identity
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the superclass's hash
        if structure is None:
            return super().__hash__()

        # Generate a hash and convert to integer
        hash_hex = self._hash_structure(structure, char_count=None)
        return int(hash_hex[:16], 16)

    def _hash_structure(self, structure: Any, char_count: Optional[int] = 16) -> str:
        """
        Helper method to compute a hash string from a structure.

        Args:
            structure: The structure to hash

        Returns:
            str: A hexadecimal hash digest of the structure
        """
        processed = self._process_structure(structure)
        json_str = json.dumps(processed, sort_keys=True).encode()
        return hashlib.sha256(json_str).hexdigest()[:char_count]

    def _process_structure(self, obj: Any) -> Any:
        """
        Recursively process a structure to prepare it for hashing.

        Args:
            obj: The object or structure to process

        Returns:
            A processed version of the structure with HashableMixin objects replaced by their hashes
        """
        # Handle None
        if obj is None:
            return "None"

        # If the object is a HashableMixin, use its content_hash
        if isinstance(obj, HashableMixin):
            # Don't call content_hash on self to avoid cycles
            if obj is self:
                # Use the superclass's hash for self
                return str(super(HashableMixin, self).__hash__())
            return obj.content_hash()

        # Handle basic types
        if isinstance(obj, (str, int, float, bool)):
            return str(obj)

        # Handle named tuples (which are subclasses of tuple)
        if hasattr(obj, "_fields") and isinstance(obj, tuple):
            # For namedtuples, convert to dict and then process
            return self._process_structure(
                {field: value for field, value in zip(obj._fields, obj)}
            )

        # Handle mappings (dict-like objects)
        if isinstance(obj, Mapping):
            return {
                str(k): self._process_structure(v)
                for k, v in sorted(obj.items(), key=lambda x: str(x[0]))
            }

        # Handle sets and frozensets specifically
        if isinstance(obj, (set, frozenset)):
            # Process each item first, then sort the processed results
            processed_items = [self._process_structure(item) for item in obj]
            return sorted(processed_items, key=str)

        # Handle collections (list-like objects)
        if isinstance(obj, Collection):
            return [self._process_structure(item) for item in obj]

        # For bytes and bytearray, convert to hex representation
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()

        # For other objects, just use their string representation
        return str(obj)


class Operation(HashableMixin):
    """
    Operation defines a generic operation that can be performed on a stream of data.
    It is a base class for all operations that can be performed on a collection of streams
    (including an empty collection).
    The operation is defined as a callable that takes a collection of streams as input
    and returns a new stream as output.
    Each invocation of the operation is assigned a unique ID. The corresponding invocation
    information is stored as Invocation object and attached to the output stream.
    """

    def __init__(self, label: Optional[str] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._label = label

    @property
    def label(self) -> Optional[str]:
        """
        The label of the operation. This is used to refer to the operation in the tracker
        """
        if self._label is None:
            # if the label is not set, use the class name as the label
            self._label = self.__class__.__name__

        return self._label

    def identity_structure(self, *streams: "SyncStream") -> Any:
        # Default implementation of identity_structure for the operation only
        # concerns the operation class and the streams if present. Subclasses of
        # Operations should override this method to provide a more meaningful
        # representation of the operation.
        return (self.__class__.__name__, streams)

    def __call__(self, *streams: "SyncStream") -> "SyncStream":
        # trigger call on source if passed as stream

        streams = [
            stream() if isinstance(stream, Source) else stream for stream in streams
        ]
        output_stream = self.forward(*streams)
        # create an invocation instance
        invocation = Invocation(self, streams)
        # label the output_stream with the invocation information
        output_stream.invocation = invocation

        # delay import to avoid circular import
        from .tracker import Tracker

        # reg
        active_trackers = Tracker.get_active_trackers()
        for tracker in active_trackers:
            tracker.record(invocation)

        return output_stream

    def __repr__(self):
        return self.__class__.__name__

    def forward(self, *streams: "SyncStream") -> "SyncStream": ...


class Invocation(HashableMixin):
    """
    This class represents an invocation of an operation on a collection of streams.
    It contains the operation, the invocation ID, and the streams that were used
    in the invocation.
    The invocation ID is a unique identifier for the invocation and is used to
    track the invocation in the tracker.
    """

    def __init__(
        self,
        operation: Operation,
        streams: Collection["SyncStream"],
    ) -> None:
        self.operation = operation
        self.streams = streams

    # @property
    # def invocation_id(self) -> int:
    #     """
    #     The invocation ID is a unique identifier for the invocation.
    #     It is used to track the invocation in the tracker.
    #     """
    #     return hash(self)

    def __hash__(self) -> int:
        return super().__hash__()

    def __repr__(self) -> str:
        return f"Invocation({self.operation}, ID:{hash(self)})"

    def identity_structure(self) -> int:
        # default implementation is streams order sensitive. If an operation does
        # not depend on the order of the streams, it should override this method
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


class Stream(HashableMixin):
    """
    A stream is a collection of tagged-packets that are generated by an operation.
    The stream is iterable and can be used to access the packets in the stream.

    A stream has propery `invocation` that is an instance of Invocation that generated the stream.
    This may be None if the stream is not generated by an operation.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._invocation: Optional[Invocation] = None

    def identity_structure(self) -> Any:
        if self.invocation is not None:
            return self.invocation.identity_structure()
        return super().identity_structure()

    @property
    def invocation(self) -> Optional[Invocation]:
        return self._invocation

    @invocation.setter
    def invocation(self, value: Invocation) -> None:
        if not isinstance(value, Invocation):
            raise TypeError("invocation field must be an instance of Invocation")
        self._invocation = value

    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        raise NotImplementedError("Subclasses must implement __iter__ method")


class SyncStream(Stream):
    """
    A stream that will complete in a fixed amount of time. It is suitable for synchronous operations that
    will have to wait for the stream to finish before proceeding.
    """

    def content_hash(self) -> str:
        if (
            self.invocation is not None
        ):  # and hasattr(self.invocation, "invocation_id"):
            # use the invocation ID as the hash
            return self.invocation.content_hash()
        return super().content_hash()

    def __hash__(self) -> int:
        return hash(self.content_hash())

    def keys(self) -> Tuple[List[str], List[str]]:
        """
        Returns the keys of the stream.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned on based-effort basis, and this invocation may trigger the
        upstream computation of the stream.
        Furthermore, the keys are not guaranteed to be identical across all packets in the stream.
        This method is useful for inferring the keys of the stream without having to iterate
        over the entire stream.
        """
        tag, packet = next(iter(self))
        return list(tag.keys()), list(packet.keys())

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

    def __rshift__(self, transformer: Any) -> "SyncStream":
        """
        Returns a new stream that is the result of applying the mapping to the stream.
        The mapping is applied to each packet in the stream and the resulting packets
        are returned in a new stream.
        """
        from .mapper import MapPackets

        # TODO: extend to generic mapping
        if isinstance(transformer, dict):
            return MapPackets(transformer)(self)
        elif isinstance(transformer, Callable):
            return transformer(self)

    def __mul__(self, other: "SyncStream") -> "SyncStream":
        """
        Returns a new stream that is the result joining with the other stream
        """
        from .mapper import Join

        if not isinstance(other, SyncStream):
            raise TypeError("other must be a SyncStream")
        return Join()(self, other)


class Source(Operation, SyncStream):
    """
    A base class for all sources in the system. A source can be seen as a special
    type of Operation that takes no input and produces a stream of packets.
    For convenience, the source itself is also a stream and thus can be used
    as an input to other operations directly.
    """

    def __init__(self, label: Optional[str] = None, **kwargs) -> None:
        super().__init__(label=label, **kwargs)
        self._invocation = None

    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        yield from self()
