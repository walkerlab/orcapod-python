from collections.abc import Callable, Collection, Iterator

import polars as pl

from orcapod.core.base import SyncStream
from orcapod.types import Packet, PacketLike, Tag, TypeSpec
from copy import copy


class SyncStreamFromLists(SyncStream):
    def __init__(
        self,
        tags: Collection[Tag] | None = None,
        packets: Collection[PacketLike] | None = None,
        paired: Collection[tuple[Tag, PacketLike]] | None = None,
        tag_keys: list[str] | None = None,
        packet_keys: list[str] | None = None,
        tag_typespec: TypeSpec | None = None,
        packet_typespec: TypeSpec | None = None,
        strict: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tag_typespec = tag_typespec
        self.packet_typespec = packet_typespec
        if tag_keys is None and tag_typespec is not None:
            tag_keys = list(tag_typespec.keys())
        if packet_keys is None and packet_typespec is not None:
            packet_keys = list(packet_typespec.keys())
        self.tag_keys = tag_keys
        self.packet_keys = packet_keys

        if tags is not None and packets is not None:
            if strict and len(tags) != len(packets):
                raise ValueError(
                    "tags and packets must have the same length if both are provided"
                )
            self.paired = list((t, Packet(v)) for t, v in zip(tags, packets))
        elif paired is not None:
            self.paired = list((t, Packet(v)) for t, v in paired)
        else:
            raise ValueError(
                "Either tags and packets or paired must be provided to SyncStreamFromLists"
            )

    def keys(
        self, *, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        tag_keys, packet_keys = copy(self.tag_keys), copy(self.packet_keys)
        if tag_keys is None or packet_keys is None:
            super_tag_keys, super_packet_keys = super().keys(trigger_run=trigger_run)
            tag_keys = tag_keys or super_tag_keys
            packet_keys = packet_keys or super_packet_keys

        # If the keys are already set, return them
        return tag_keys, packet_keys

    def types(
        self, *, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        tag_typespec, packet_typespec = (
            copy(self.tag_typespec),
            copy(self.packet_typespec),
        )
        if tag_typespec is None or packet_typespec is None:
            super_tag_typespec, super_packet_typespec = super().types(
                trigger_run=trigger_run
            )
            tag_typespec = tag_typespec or super_tag_typespec
            packet_typespec = packet_typespec or super_packet_typespec

        # If the types are already set, return them
        return tag_typespec, packet_typespec

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
        self.check_consistency = False

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        if not self.check_consistency:
            yield from self.generator_factory()

    # TODO: add typespec handling
    def keys(
        self, *, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        if self.tag_keys is None or self.packet_keys is None:
            return super().keys(trigger_run=trigger_run)
        # If the keys are already set, return them
        return self.tag_keys.copy(), self.packet_keys.copy()


class PolarsStream(SyncStream):
    def __init__(
        self,
        df: pl.DataFrame,
        tag_keys: Collection[str],
        packet_keys: Collection[str] | None = None,
    ):
        self.df = df
        self.tag_keys = tuple(tag_keys)
        self.packet_keys = tuple(packet_keys) if packet_keys is not None else None

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        df = self.df
        # if self.packet_keys is not None:
        #     df = df.select(self.tag_keys + self.packet_keys)
        for row in df.iter_rows(named=True):
            tag = {key: row[key] for key in self.tag_keys}
            packet = {
                key: val
                for key, val in row.items()
                if key not in self.tag_keys and not key.startswith("_source_info_")
            }
            # TODO: revisit and fix this rather hacky implementation
            source_info = {
                key.removeprefix("_source_info_"): val
                for key, val in row.items()
                if key.startswith("_source_info_")
            }
            yield tag, Packet(packet, source_info=source_info)


class EmptyStream(SyncStream):
    def __init__(
        self,
        tag_keys: Collection[str] | None = None,
        packet_keys: Collection[str] | None = None,
        tag_typespec: TypeSpec | None = None,
        packet_typespec: TypeSpec | None = None,
    ):
        if tag_keys is None and tag_typespec is not None:
            tag_keys = tag_typespec.keys()
        self.tag_keys = list(tag_keys) if tag_keys else []

        if packet_keys is None and packet_typespec is not None:
            packet_keys = packet_typespec.keys()
        self.packet_keys = list(packet_keys) if packet_keys else []

        self.tag_typespec = tag_typespec
        self.packet_typespec = packet_typespec

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.tag_keys, self.packet_keys

    def types(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        return self.tag_typespec, self.packet_typespec

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        # Empty stream, no data to yield
        return iter([])


class StreamWrapper(SyncStream):
    """
    A wrapper for a SyncStream that allows the stream to be labeled and
    associated with an invocation without modifying the original stream.
    """

    def __init__(self, stream: SyncStream, **kwargs):
        super().__init__(**kwargs)
        self.stream = stream

    def keys(
        self, *streams: SyncStream, **kwargs
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.stream.keys(*streams, **kwargs)

    def types(
        self, *streams: SyncStream, **kwargs
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        return self.stream.types(*streams, **kwargs)

    def computed_label(self) -> str | None:
        return self.stream.label

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Iterate over the stream, yielding tuples of (tags, packets).
        """
        yield from self.stream
