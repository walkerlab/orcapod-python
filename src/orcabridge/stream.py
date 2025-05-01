from typing import Generator, Tuple, Dict, Any, Callable, Iterator, Optional
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


class SyncStreamFromGenerator(SyncStream):
    """
    A synchronous stream that is backed by a generator function.
    """

    def __init__(
        self, generator_factory: Callable[[], Iterator[Tuple[Tag, Packet]]]
    ) -> None:
        super().__init__()
        self.generator_factory = generator_factory

    def __iter__(self) -> Iterator[Tuple[Tag, Packet]]:
        yield from self.generator_factory()

    def __len__(self) -> int:
        return sum(1 for _ in self)
