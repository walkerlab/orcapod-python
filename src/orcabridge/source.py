from orcabridge.types import Tag, Packet
from orcabridge.hashing import hash_function
from orcabridge.base import Source
from orcabridge.stream import SyncStream, SyncStreamFromGenerator
from typing import Any, Literal
from os import PathLike
from pathlib import Path
from collections.abc import Collection, Iterator, Callable


class LoadFromSource(Source):
    pass


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

    default_tag_function = lambda f: {"file_name": Path(f).stem}  # noqa: E731

    def __init__(
        self,
        name: str,
        file_path: PathLike,
        pattern: str = "*",
        label: str | None = None,
        tag_function: str | Callable[[PathLike], Tag] | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        expected_tag_keys: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.name = name
        self.file_path = file_path
        self.pattern = pattern
        self.expected_tag_keys = expected_tag_keys
        if self.expected_tag_keys is None and isinstance(tag_function, str):
            self.expected_tag_keys = [tag_function]
        if tag_function is None:
            tag_function = self.__class__.default_tag_function
        elif isinstance(tag_function, str):
            tag_key = tag_function
            tag_function = lambda f: {tag_key: Path(f).stem}  # noqa: E731
        self.tag_function: Callable[[PathLike], Tag] = tag_function
        self.tag_function_hash_mode = tag_function_hash_mode

    def keys(
        self, *streams: SyncStream
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
        return super().keys()

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "GlobSource does not support forwarding streams. "
                "It generates its own stream from the file system."
            )

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for file in Path(self.file_path).glob(self.pattern):
                yield self.tag_function(file), {self.name: str(file)}

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
