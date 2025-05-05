from .base import Source
from .stream import SyncStream, SyncStreamFromGenerator
from .types import Tag, Packet
from typing import Iterator, Tuple, Optional, Callable, Any
from os import PathLike
from pathlib import Path
from .utils.hash import function_content_hash, stable_hash


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
    tag_function : Optional[Callable[[PathLike], Tag]], default=None
        Optional function to generate a tag from a file path. If None, uses the file's
        stem name (without extension) in a dict with key 'file_name'

    Examples
    --------
    >>> # Match all .txt files in data_dir, using filename as tag
    >>> glob_source = GlobSource('txt_file', 'data_dir', '*.txt')
    >>> # Match all files but use custom tag function
    >>> glob_source = GlobSource('file', 'data_dir', '*',
    ...                     lambda f: {'date': Path(f).stem[:8]})
    """

    default_tag_function = lambda f: {"file_name": Path(f).stem}

    def __init__(
        self,
        name: str,
        file_path: PathLike,
        pattern: str = "*",
        label: Optional[str] = None,
        tag_function: Optional[Callable[[PathLike], Tag]] = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.name = name
        self.file_path = file_path
        self.pattern = pattern
        if tag_function is None:
            # extract the file name without extension
            tag_function = self.__class__.default_tag_function
        self.tag_function = tag_function

    def forward(self) -> SyncStream:
        def generator() -> Iterator[Tuple[Tag, Packet]]:
            for file in Path(self.file_path).glob(self.pattern):
                yield self.tag_function(file), {self.name: file}

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"GlobSource({str(Path(self.file_path) / self.pattern)}) ⇒ {self.name}"

    def identity_structure(self, *streams) -> Any:
        return (
            self.__class__.__name__,
            self.name,
            str(self.file_path),
            self.pattern,
            function_content_hash(self.tag_function),
        ) + tuple(streams)
