from typing import Generator, Tuple, Dict, Any, Callable, Iterator, Optional, List
from .types import Tag, Packet
from .operation import Operation, Invocation



class Stream:
    def __init__(self):
        self._source: Optional[Invocation] = None

    @property
    def source(self) -> Optional[Invocation]:
        return self._source
    

    @source.setter
    def source(self, value: Invocation) -> None:
        if not isinstance(value, Invocation):
            raise TypeError("source must be an instance of Invocation")
        self._source = value


    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        raise NotImplementedError("Subclasses must implement __iter__ method")


class SyncStream(Stream):
    """
    A stream that will complete in a fixed amount of time. It is suitable for synchronous operations that
    will have to wait for the stream to finish before proceeding.
    """
    def __hash__(self) -> int:
        if hasattr(self, 'source') and self.source is not None:
            return hash(self.source)
        return super().__hash__()

    
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
    
    def preview(self, n: int = 1) -> None:
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
        return sum(1 for _ in self)


class SyncStreamFromGenerator(SyncStream):
    """
    A synchronous stream that is backed by a generator function.
    """

    def __init__(
        self, generator_factory: Callable[[], Iterator[Tuple[Tag, Packet]]], tag_keys: Optional[List[str]] = None, packet_keys: Optional[List[str]] = None,
    ) -> None:
        super().__init__()
        self.tag_keys = tag_keys
        self.packet_keys = packet_keys
        self.generator_factory = generator_factory

    def keys(self) -> Tuple[List[str], List[str]]:
        if self.tag_keys is None or self.packet_keys is None:
            return super().keys()
        # If the keys are already set, return them
        return self.tag_keys.copy(), self.packet_keys.copy()  

    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        yield from self.generator_factory()

 
