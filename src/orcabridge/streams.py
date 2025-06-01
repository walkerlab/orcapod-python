from collections.abc import Callable, Collection, Iterator

from orcabridge.base import SyncStream
from orcabridge.types import Packet, Tag


class SyncStreamFromLists(SyncStream):
    def __init__(
        self,
        tags: Collection[Tag] | None = None,
        packets: Collection[Packet] | None = None,
        paired: Collection[tuple[Tag, Packet]] | None = None,
        tag_keys: list[str] | None = None,
        packet_keys: list[str] | None = None,
        strict: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag_keys = tag_keys
        self.packet_keys = packet_keys
        if tags is not None and packets is not None:
            if strict and len(tags) != len(packets):
                raise ValueError(
                    "tags and packets must have the same length if both are provided"
                )
            self.paired = list(zip(tags, packets))
        elif paired is not None:
            self.paired = list(paired)
        else:
            raise ValueError(
                "Either tags and packets or paired must be provided to SyncStreamFromLists"
            )

    def keys(self) -> tuple[Collection[str] | None, Collection[str] | None]:
        if self.tag_keys is None or self.packet_keys is None:
            return super().keys()
        # If the keys are already set, return them
        return self.tag_keys.copy(), self.packet_keys.copy()

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        yield from self.paired


class SyncStreamFromGenerator(SyncStream):
    """
    A synchronous stream that is backed by a generator function.
    """

    def __init__(
        self,
        generator_factory: Callable[[], Iterator[tuple[Tag, Packet]]],
        tag_keys: list[str] | None = None,
        packet_keys: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag_keys = tag_keys
        self.packet_keys = packet_keys
        self.generator_factory = generator_factory

    def keys(self) -> tuple[Collection[str] | None, Collection[str] | None]:
        if self.tag_keys is None or self.packet_keys is None:
            return super().keys()
        # If the keys are already set, return them
        return self.tag_keys.copy(), self.packet_keys.copy()

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        yield from self.generator_factory()
