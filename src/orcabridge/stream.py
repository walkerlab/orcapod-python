from typing import Generator, Tuple, Dict, Any, Callable, Iterator
from .types import Tag, Packet

class Stream():
    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        raise NotImplementedError("Subclasses must implement __iter__ method")

class SyncStream(Stream):
    """
    A stream that will complete in a fixed amount of time. It is suitable for synchronous operations that
    will have to wait for the stream to finish before proceeding.
    """
    

class SyncStreamFromGenerator(SyncStream):
    """
    A synchronous stream that is backed by a generator function.
    """

    def __init__(self, generator_factory: Callable[[], Iterator[Tuple[Tag, Packet]]]) -> None:
        self.generator_factory = generator_factory

    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        yield from self.generator_factory()
    
    def __len__(self) -> int:
        return sum(1 for _ in self)
    
    

