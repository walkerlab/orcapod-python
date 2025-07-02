from collections.abc import Callable, Collection, Iterator
from os import PathLike
from pathlib import Path
from typing import Any, Literal

import polars as pl

from orcapod.core.base import Source
from orcapod.hashing.legacy_core import hash_function
from orcapod.core.streams import (
    PolarsStream,
    SyncStream,
    SyncStreamFromGenerator,
    StreamWrapper,
)
from orcapod.types import Packet, Tag, TypeSpec


class GlobSource(Source):
    """
    A stream source that sources files from a directory matching a glob pattern.

    For each matching file, yields a tuple containing:
    - A tag generated either by the provided tag_function or defaulting to the file's stem name
    - A packet containing the file path under the provided name key

    Parameters
    ----------
    name : str
        The key name under which the file path will be stored in the packet
    file_path : PathLike
        The directory path to search for files
    pattern : str, default='*'
        The glob pattern to match files against
    tag_key : Optional[Union[str, Callable[[PathLike], Tag]]], default=None
        Optional function to generate a tag from a file path. If None, uses the file's
        stem name (without extension) in a dict with key 'file_name'. If only string is
        provided, it will be used as the key for the tag. If a callable is provided, it
        should accept a file path and return a dictionary of tags.

    Examples
    --------
    >>> # Match all .txt files in data_dir, using filename as tag
    >>> glob_source = GlobSource('txt_file', 'data_dir', '*.txt')
    >>> # Match all files but use custom tag function
    >>> glob_source = GlobSource('file', 'data_dir', '*',
    ...                     lambda f: {'date': Path(f).stem[:8]})
    """

    @staticmethod
    def default_tag_function(f: PathLike) -> Tag:
        return {"file_name": Path(f).stem}  # noqa: E731

    def __init__(
        self,
        name: str,
        file_path: PathLike,
        pattern: str = "*",
        absolute_path: bool = False,
        label: str | None = None,
        tag_function: Callable[[PathLike], Tag] | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        expected_tag_keys: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.name = name
        file_path = Path(file_path)
        if absolute_path:
            file_path = file_path.resolve()
        self.file_path = file_path
        self.pattern = pattern
        self.expected_tag_keys = expected_tag_keys
        if tag_function is None:
            tag_function = self.__class__.default_tag_function
        self.tag_function: Callable[[PathLike], Tag] = tag_function
        self.tag_function_hash_mode = tag_function_hash_mode

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "GlobSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for file in Path(self.file_path).glob(self.pattern):
                yield self.tag_function(file), Packet({self.name: str(file)})

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"GlobSource({str(Path(self.file_path) / self.pattern)}) ⇒ {self.name}"

    def identity_structure(self, *streams) -> Any:
        hash_function_kwargs = {}
        if self.tag_function_hash_mode == "content":
            # if using content hash, exclude few
            hash_function_kwargs = {
                "include_name": False,
                "include_module": False,
                "include_declaration": False,
            }

        tag_function_hash = hash_function(
            self.tag_function,
            function_hash_mode=self.tag_function_hash_mode,  # type: ignore
            hash_kwargs=hash_function_kwargs,
        )
        return (
            self.__class__.__name__,
            self.name,
            str(self.file_path),
            self.pattern,
            tag_function_hash,
        ) + tuple(streams)

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the stream. The keys are the names of the packets
        in the stream. The keys are used to identify the packets in the stream.
        If expected_keys are provided, they will be used instead of the default keys.
        """
        if len(streams) != 0:
            raise ValueError(
                "GlobSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )

        if self.expected_tag_keys is not None:
            return tuple(self.expected_tag_keys), (self.name,)
        return super().keys(trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: "SyncStream", trigger_run: bool = True
    ) -> bool | None:
        if len(streams) != 0:
            raise ValueError(
                "GlobSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )
        # Claim uniqueness only if the default tag function is used
        if self.tag_function == self.__class__.default_tag_function:
            return True
        # Otherwise, delegate to the base class
        return super().claims_unique_tags(trigger_run=trigger_run)


class PolarsSource(Source):
    def __init__(
        self,
        df: pl.DataFrame,
        tag_keys: Collection[str],
        packet_keys: Collection[str] | None = None,
    ):
        self.df = df
        self.tag_keys = tag_keys
        self.packet_keys = packet_keys

    def forward(self, *streams: SyncStream, **kwargs) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "PolarsSource does not support forwarding streams. "
                "It generates its own stream from the DataFrame."
            )
        return PolarsStream(self.df, self.tag_keys, self.packet_keys)


class StreamSource(Source):
    def __init__(self, stream: SyncStream, **kwargs):
        super().__init__(skip_tracking=True, **kwargs)
        self.stream = stream

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "StreamSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )
        return StreamWrapper(self.stream)

    def identity_structure(self, *streams) -> Any:
        if len(streams) != 0:
            raise ValueError(
                "StreamSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )

        return (self.__class__.__name__, self.stream)

    def types(
        self, *streams: SyncStream, **kwargs
    ) -> tuple[TypeSpec | None, TypeSpec | None]:
        return self.stream.types()

    def keys(
        self, *streams: SyncStream, **kwargs
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        return self.stream.keys()

    def computed_label(self) -> str | None:
        return self.stream.label
