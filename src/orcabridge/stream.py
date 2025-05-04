from typing import Generator, Tuple, Dict, Any, Callable, Iterator, Optional, List
from .types import Tag, Packet
from .base import SyncStream


class SyncStreamFromGenerator(SyncStream):
    """
    A synchronous stream that is backed by a generator function.
    """

    def __init__(
        self,
        generator_factory: Callable[[], Iterator[Tuple[Tag, Packet]]],
        tag_keys: Optional[List[str]] = None,
        packet_keys: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
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
