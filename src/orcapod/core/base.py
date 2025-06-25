# Collection of base classes for operations and streams in the orcabridge framework.
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Iterator
from typing import Any


from orcapod.hashing import HashableMixin, ObjectHasher
from orcapod.hashing import get_default_object_hasher

from orcapod.hashing import ContentHashableBase
from orcapod.types import Packet, Tag, TypeSpec
from orcapod.utils.stream_utils import get_typespec

import logging


logger = logging.getLogger(__name__)


class Kernel(ABC, ContentHashableBase):
    """
    Kernel defines the fundamental unit of computation that can be performed on zero, one or more streams of data.
    It is the base class for all computations and transformations that can be performed on a collection of streams
    (including an empty collection).
    A kernel is defined as a callable that takes a (possibly empty) collection of streams as the input
    and returns a new stream as output (note that output stream is always singular).
    Each "invocation" of the kernel on a collection of streams is assigned a unique ID.
    The corresponding invocation information is stored as Invocation object and attached to the output stream
    for computational graph tracking.
    """

    def __init__(self, label: str | None = None, skip_tracking: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self._label = label
        self._skip_tracking = skip_tracking



    def pre_forward_hook(
        self, *streams: "SyncStream", **kwargs
    ) -> tuple["SyncStream", ...]:
        """
        A hook that is called before the forward method is invoked.
        This can be used to perform any pre-processing or validation on the input streams.
        Subclasses can override this method to provide custom behavior.
        """
        return streams

    def post_forward_hook(self, output_stream: "SyncStream", **kwargs) -> "SyncStream":
        """
        A hook that is called after the forward method is invoked.
        This can be used to perform any post-processing on the output stream.
        Subclasses can override this method to provide custom behavior.
        """
        return output_stream

   
    def __call__(self, *streams: "SyncStream", label:str|None = None, **kwargs) -> "SyncStream":
        if label is not None:
            self.label = label
        # Special handling of Source: trigger call on source if passed as stream
        normalized_streams = [
            stream() if isinstance(stream, Source) else stream for stream in streams
        ]

        pre_processed_streams = self.pre_forward_hook(*normalized_streams, **kwargs)
        output_stream = self.forward(*pre_processed_streams, **kwargs)
        post_processed_stream = self.post_forward_hook(output_stream, **kwargs)
        # create an invocation instance
        invocation = Invocation(self, pre_processed_streams)
        # label the output_stream with the invocation that produced the stream
        post_processed_stream.invocation = invocation

        if not self._skip_tracking:
            # register the invocation to all active trackers
            active_trackers = Tracker.get_active_trackers()
            for tracker in active_trackers:
                tracker.record(invocation)

        return post_processed_stream

    @abstractmethod
    def forward(self, *streams: "SyncStream") -> "SyncStream":
        """
        Trigger the main computation of the kernel on a collection of streams.
        This method is called when the kernel is invoked with a collection of streams.
        Subclasses should override this method to provide the kernel with its unique behavior
        """

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        if self._label is not None:
            return f"{self.__class__.__name__}({self._label})"
        return self.__class__.__name__

    def identity_structure(self, *streams: "SyncStream") -> Any:
        # Default implementation of identity_structure for the kernel only
        # concerns the kernel class and the streams if present. Subclasses of
        # Kernels should override this method to provide a more meaningful
        # representation of the kernel. Note that kernel must provide the notion
        # of identity under possibly two distinct contexts:
        # 1) identity of the kernel in itself when invoked without any stream
        # 2) identity of the specific invocation of the kernel with a collection of streams
        # While the latter technically corresponds to the identity of the invocation and not
        # the kernel, only kernel can provide meaningful information as to the uniqueness of
        # the invocation as only kernel would know if / how the input stream(s) alter the identity
        # of the invocation. For example, if the kernel corresponds to an commutative computation
        # and therefore kernel K(x, y) == K(y, x), then the identity structure must reflect the
        # equivalence of the two by returning the same identity structure for both invocations.
        # This can be achieved, for example, by returning a set over the streams instead of a tuple.
        logger.warning(
            f"Identity structure not implemented for {self.__class__.__name__}"
        )
        return (self.__class__.__name__,) + streams

    def keys(
        self, *streams: "SyncStream", trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the kernel output.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        If trigger_run is False (default), the keys are returned only if it is feasible to do so without triggering
        the chain of computations. If trigger_run is True, underlying computation may get triggered if doing so
        would allow for the keys to be determined. Returns None for either part of the keys cannot be inferred.

        This should be overridden by the subclass if subclass can provide smarter inference based on the specific
        implementation of the subclass and input streams.
        """
        if not trigger_run:
            return None, None

        # resolve to actually executing the stream to fetch the first element
        tag, packet = next(iter(self(*streams)))
        return tuple(tag.keys()), tuple(packet.keys())

    def types(
        self, *streams: "SyncStream", trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Returns the tag and packet typespec of the kernel output.
        Each typespec consists of mapping from field name to Python type.
        If trigger_run is False (default), the typespec info is returned only if it is feasible to do so without triggering
        the chain of computation. If trigger_run is True, underlying computation may get triggered if doing so
        would allow for the typespec to be determined. Returns None for either part of the typespec cannot be inferred.
        """
        if not trigger_run:
            return None, None

        tag, packet = next(iter(self(*streams)))
        return get_typespec(tag), get_typespec(packet)

    def claims_unique_tags(
        self, *streams: "SyncStream", trigger_run: bool = False
    ) -> bool | None:
        """
        Returns True if the kernel claims that it has unique tags, False otherwise.
        False indicates that it can be inferred that the kernel does not have unique tags
        based on the input streams and the kernel's implementation. None indicates that
        whether it is unique or not cannot be determined with certainty.
        If trigger_run is True, the kernel may trigger the computation to verify
        the uniqueness of tags. If trigger_run is False, the kernel will return
        None if it cannot determine the uniqueness of tags without triggering the computation.
        This method is useful for checking if the kernel can be used as a source
        for other kernels that require unique tags.
        Subclasses should override this method if it can provide reasonable check/guarantee
        of unique tags. The default implementation returns False, meaning that the kernel
        does not claim to have unique tags, even if turns out to be unique.
        """
        return None


class Tracker(ABC):
    """
    A tracker is a class that can track the invocations of kernels. Only "active" trackers
    participate in tracking and its `record` method gets called on each invocation of a kernel.
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
# represents an invocation of a kernel on a collection of streams.
class Invocation(ContentHashableBase):
    """
    This class represents an invocation of a kernel on a collection of streams.
    It contains the kernel and the streams that were used in the invocation.
    Note that the collection of streams may be empty, in which case the invocation
    likely corresponds to a source kernel.
    """

    def __init__(
        self,
        kernel: Kernel,
        # TODO: technically this should be Stream to stay consistent with Stream interface. Update to Stream when AsyncStream is implemented
        streams: Collection["SyncStream"],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.kernel = kernel
        self.streams = streams

    def computed_label(self) -> str | None:
        """
        Returns the computed label for this invocation.
        This is used to provide a default label if no label is set.
        """
        return self.kernel.label

    def __repr__(self) -> str:
        return f"Invocation(kernel={self.kernel}, streams={self.streams})"

    def __str__(self) -> str:
        return f"Invocation[ID:{self.__hash__()}]({self.kernel}, {self.streams})"

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Invocation):
            return NotImplemented

        if self.kernel == other.kernel:
            return hash(self) < hash(other)
        # otherwise, order by the kernel
        return hash(self.kernel) < hash(other.kernel)

    # Pass-through implementations: these methods are implemented by "passing-through" the methods logic,
    # simply invoking the corresopnding methods on the underlying kernel with the input streams

    def claims_unique_tags(self, trigger_run: bool = True) -> bool | None:
        """
        Returns True if the invocation has unique tags, False otherwise.
        This method is useful for checking if the invocation can be used as a source
        for other kernels that require unique tags. None is returned if the
        uniqueness of tags cannot be determined.
        Note that uniqueness is best thought of as a "claim" by the kernel
        that it has unique tags. The actual uniqueness can only be verified
        by iterating over the streams and checking the tags.
        """
        return self.kernel.claims_unique_tags(*self.streams, trigger_run=trigger_run)

    def keys(self) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.kernel.keys(*self.streams)

    def types(self) -> tuple[TypeSpec | None, TypeSpec | None]:
        return self.kernel.types(*self.streams)

    def identity_structure(self) -> int:
        # Identity of an invocation is entirely determined by the
        # the kernel's identity structure upon invocation
        return self.kernel.identity_structure(*self.streams)


class Stream(ABC, ContentHashableBase):
    """
    A stream is a collection of tagged-packets that are generated by an operation.
    The stream is iterable and can be used to access the packets in the stream.

    A stream has property `invocation` that is an instance of Invocation that generated the stream.
    This may be None if the stream is not generated by a kernel (i.e. directly instantiated by a user).
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._invocation: Invocation | None = None

    def computed_label(self) -> str | None:
        if self.invocation is not None:
            # use the invocation operation label
            return self.invocation.kernel.label
        return None
    

    @property
    def invocation(self) -> Invocation | None:
        return self._invocation

    @invocation.setter
    def invocation(self, value: Invocation) -> None:
        if not isinstance(value, Invocation):
            raise TypeError("invocation field must be an instance of Invocation")
        self._invocation = value

    @abstractmethod
    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        raise NotImplementedError("Subclasses must implement __iter__ method")

    def flow(self) -> Collection[tuple[Tag, Packet]]:
        """
        Flow everything through the stream, returning the entire collection of
        (Tag, Packet) as a collection. This will tigger any upstream computation of the stream.
        """
        return [e for e in self]

    # --------------------- Recursive methods ---------------------------
    # These methods form a step in the multi-class recursive invocation that follows the pattern of
    # Stream -> Invocation -> Kernel -> Stream ... -> Invocation -> Kernel
    # Most of the method logic would be found in Kernel's implementation of the method with
    # Stream and Invocation simply serving as recursive steps

    def identity_structure(self) -> Any:
        """
        Identity structure of a stream is deferred to the identity structure
        of the associated invocation, if present.
        A bare stream without invocation has no well-defined identity structure.
        Specialized stream subclasses should override this method to provide more meaningful identity structure
        """
        if self.invocation is not None:
            return self.invocation.identity_structure()
        return super().identity_structure()

    def keys(
        self, *, trigger_run=False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
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
        if not trigger_run:
            return None, None
        # otherwise, use the keys from the first packet in the stream
        # note that this may be computationally expensive
        tag, packet = next(iter(self))
        return list(tag.keys()), list(packet.keys())

    def types(self, *, trigger_run=False) -> tuple[TypeSpec | None, TypeSpec | None]:
        """
        Returns the keys of the stream.
        The first list contains the keys of the tags, and the second list contains the keys of the packets.
        The keys are returned on based-effort basis, and this invocation may trigger the
        upstream computation of the stream.
        Furthermore, the keys are not guaranteed to be identical across all packets in the stream.
        This method is useful for inferring the keys of the stream without having to iterate
        over the entire stream.
        """
        tag_types, packet_types = None, None
        if self.invocation is not None:
            # if the stream is generated by an operation, use the keys from the invocation
            tag_types, packet_types = self.invocation.types()
            if not trigger_run or (tag_types is not None and packet_types is not None):
                return tag_types, packet_types
        if not trigger_run:
            return None, None
        # otherwise, use the keys from the first packet in the stream
        # note that this may be computationally expensive
        tag, packet = next(iter(self))
        return tag_types or get_typespec(tag), packet_types or get_typespec(packet)

    def claims_unique_tags(self, *, trigger_run=False) -> bool | None:
        """
        Returns True if the stream has unique tags, False otherwise.
        This method is useful for checking if the stream can be used as a source
        for other operations that require unique tags. None is returned if the
        uniqueness of tags cannot be determined.
        If the stream is generated by an operation, the invocation is consulted for
        the information about unique tags.
        """
        if self.invocation is not None:
            return self.invocation.claims_unique_tags(trigger_run=trigger_run)
        return None


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
        self, transformer: dict | Callable[["SyncStream"], "SyncStream"]
    ) -> "SyncStream":
        """
        Returns a new stream that is the result of applying the mapping to the stream.
        The mapping is applied to each packet in the stream and the resulting packets
        are returned in a new stream.
        """
        # TODO: remove just in time import
        from .operators import MapPackets

        if isinstance(transformer, dict):
            return MapPackets(transformer)(self)
        elif isinstance(transformer, Callable):
            return transformer(self)

        # Otherwise, do not know how to handle the transformer
        raise TypeError(
            "transformer must be a dictionary or a callable that takes a SyncStream"
        )

    def __mul__(self, other: "SyncStream") -> "SyncStream":
        """
        Returns a new stream that is the result joining with the other stream
        """
        # TODO: remove just in time import
        from .operators import Join

        if not isinstance(other, SyncStream):
            raise TypeError("other must be a SyncStream")
        return Join()(self, other)

    def claims_unique_tags(self, *, trigger_run=False) -> bool | None:
        """
        For synchronous streams, if the stream is generated by an operation, the invocation
        is consulted first to see if the uniqueness of tags can be determined without iterating over the stream.
        If uniqueness cannot be determined from the invocation and if trigger_run is True, uniqueness is checked
        by iterating over all elements and verifying uniqueness.
        Consequently, this may trigger upstream computations and can be expensive.
        If trigger_run is False, the method will return None if the uniqueness cannot be determined.
        Since this consults the invocation, the resulting value is ultimately a claim and not a guarantee
        of uniqueness. If guarantee of uniquess is required, then use has_unique_tags method
        """
        result = super().claims_unique_tags(trigger_run=trigger_run)
        if not trigger_run or result is not None:
            return result

        # If the uniqueness cannot be determined from the invocation, iterate over the stream
        unique_tags = set()
        for tag, _ in self:
            if tag in unique_tags:
                return False
            unique_tags.add(tag)
        return True


class Source(Kernel, SyncStream):
    """
    A base class for all sources in the system. A source can be seen as a special
    type of kernel that takes no input and produces a stream of packets.
    For convenience, the source itself can act as a stream and thus can be used
    as an input to other kernels directly.
    However, note that a source is still best thought of as a kernel that
    produces a stream of packets, rather than a stream itself. On almost all occasions,
    a source acts as a kernel.
    """

    def __init__(self, label: str | None = None, **kwargs) -> None:
        super().__init__(label=label, **kwargs)
        self._invocation = None

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Simple iter method that allows for Source object to act as a stream.
        """
        yield from self()

    # TODO: consider adding stream-like behavior for determining keys and types
    def keys(
        self, *streams: "SyncStream", trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return Kernel.keys(self, *streams, trigger_run=trigger_run)

    def types(
        self, *streams: "SyncStream", trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        return Kernel.types(self, *streams, trigger_run=trigger_run)
