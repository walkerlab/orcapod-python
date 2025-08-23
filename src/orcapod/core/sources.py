from collections.abc import Callable, Collection, Iterator
from os import PathLike
from pathlib import Path
from typing import Any, Literal
import pandas as pd
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


class DataFrameSource(Source):
    """
    A stream source that sources data from a pandas DataFrame.

    For each row in the DataFrame, yields a tuple containing:
    - A tag generated either by the provided tag_function or defaulting to the row index
    - A packet containing values from specified columns as key-value pairs

    Parameters
    ----------
    columns : list[str]
        List of column names to include in the packet. These will serve as the keys
        in the packet, with the corresponding row values as the packet values.
    data : pd.DataFrame
        The pandas DataFrame to source data from
    tag_function : Callable[[pd.Series, int], Tag] | None, default=None
        Optional function to generate a tag from a DataFrame row and its index. 
        The function receives the row as a pandas Series and the row index as arguments.
        If None, uses the row index in a dict with key 'row_index'
    tag_function_hash_mode : Literal["content", "signature", "name"], default="name"
        How to hash the tag function for identity purposes
    expected_tag_keys : Collection[str] | None, default=None
        Expected tag keys for the stream
    label : str | None, default=None
        Optional label for the source

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     'file_path': ['/path/to/file1.txt', '/path/to/file2.txt'],
    ...     'metadata_path': ['/path/to/meta1.json', '/path/to/meta2.json'],
    ...     'sample_id': ['sample_1', 'sample_2']
    ... })
    >>> # Use sample_id column for tags and include file paths in packets
    >>> source = DataFrameSource(
    ...     columns=['file_path', 'metadata_path'],
    ...     data=df,
    ...     tag_function=lambda row, idx: {'sample_id': row['sample_id']}
    ... )
    >>> # Use default row index tagging
    >>> source = DataFrameSource(['file_path', 'metadata_path'], df)
    """

    @staticmethod
    def default_tag_function(row: pd.Series, idx: int) -> Tag:
        return {"row_index": idx}

    def __init__(
        self,
        columns: list[str],
        data: pd.DataFrame,
        tag_function: Callable[[pd.Series, int], Tag] | None = None,
        label: str | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        expected_tag_keys: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.columns = columns
        self.dataframe = data
        
        # Validate that all specified columns exist in the DataFrame
        missing_columns = set(columns) - set(data.columns)
        if missing_columns:
            raise ValueError(f"Columns not found in DataFrame: {missing_columns}")
        
        if tag_function is None:
            tag_function = self.__class__.default_tag_function
            # If using default tag function and no explicit expected_tag_keys, set to default
            if expected_tag_keys is None:
                expected_tag_keys = ["row_index"]
        
        self.expected_tag_keys = expected_tag_keys
        self.tag_function = tag_function
        self.tag_function_hash_mode = tag_function_hash_mode

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "DataFrameSource does not support forwarding streams. "
                "It generates its own stream from the DataFrame."
            )

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for idx, row in self.dataframe.iterrows():
                tag = self.tag_function(row, idx)
                packet = {col: row[col] for col in self.columns}
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"DataFrameSource(cols={self.columns}, rows={len(self.dataframe)})"

    def identity_structure(self, *streams: SyncStream) -> Any:
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
            function_hash_mode=self.tag_function_hash_mode,
            hash_kwargs=hash_function_kwargs,
        )
        
        # Convert DataFrame to hashable representation
        df_subset = self.dataframe[self.columns]
        df_content = df_subset.to_dict('records')
        df_hashable = tuple(tuple(sorted(record.items())) for record in df_content)
        
        return (
            self.__class__.__name__,
            tuple(self.columns),
            df_hashable,
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
                "DataFrameSource does not support forwarding streams. "
                "It generates its own stream from the DataFrame."
            )

        if self.expected_tag_keys is not None:
            return tuple(self.expected_tag_keys), tuple(self.columns)
        return super().keys(trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: "SyncStream", trigger_run: bool = True
    ) -> bool | None:
        if len(streams) != 0:
            raise ValueError(
                "DataFrameSource does not support forwarding streams. "
                "It generates its own stream from the DataFrame."
            )
        # Claim uniqueness only if the default tag function is used
        if self.tag_function == self.__class__.default_tag_function:
            return True
        # Otherwise, delegate to the base class
        return super().claims_unique_tags(trigger_run=trigger_run)


class ListSource(Source):
    """
    A stream source that sources data from a list of elements.

    For each element in the list, yields a tuple containing:
    - A tag generated either by the provided tag_function or defaulting to the element index
    - A packet containing the element under the provided name key

    Parameters
    ----------
    name : str
        The key name under which each list element will be stored in the packet
    data : list[Any]
        The list of elements to source data from
    tag_function : Callable[[Any, int], Tag] | None, default=None
        Optional function to generate a tag from a list element and its index. 
        The function receives the element and the index as arguments.
        If None, uses the element index in a dict with key 'element_index'
    tag_function_hash_mode : Literal["content", "signature", "name"], default="name"
        How to hash the tag function for identity purposes
    expected_tag_keys : Collection[str] | None, default=None
        Expected tag keys for the stream
    label : str | None, default=None
        Optional label for the source

    Examples
    --------
    >>> # Simple list of file names
    >>> file_list = ['/path/to/file1.txt', '/path/to/file2.txt', '/path/to/file3.txt']
    >>> source = ListSource('file_path', file_list)
    >>> 
    >>> # Custom tag function using filename stems
    >>> from pathlib import Path
    >>> source = ListSource(
    ...     'file_path', 
    ...     file_list,
    ...     tag_function=lambda elem, idx: {'file_name': Path(elem).stem}
    ... )
    >>> 
    >>> # List of sample IDs
    >>> samples = ['sample_001', 'sample_002', 'sample_003']
    >>> source = ListSource(
    ...     'sample_id', 
    ...     samples,
    ...     tag_function=lambda elem, idx: {'sample': elem}
    ... )
    """

    @staticmethod
    def default_tag_function(element: Any, idx: int) -> Tag:
        return {"element_index": idx}

    def __init__(
        self,
        name: str,
        data: list[Any],
        tag_function: Callable[[Any, int], Tag] | None = None,
        label: str | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        expected_tag_keys: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.name = name
        self.elements = list(data)  # Create a copy to avoid external modifications
        
        if tag_function is None:
            tag_function = self.__class__.default_tag_function
            # If using default tag function and no explicit expected_tag_keys, set to default
            if expected_tag_keys is None:
                expected_tag_keys = ["element_index"]
        
        self.expected_tag_keys = expected_tag_keys
        self.tag_function = tag_function
        self.tag_function_hash_mode = tag_function_hash_mode

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )

        def generator() -> Iterator[tuple[Tag, Packet]]:
            for idx, element in enumerate(self.elements):
                tag = self.tag_function(element, idx)
                packet = {self.name: element}
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"ListSource({self.name}, {len(self.elements)} elements)"

    def identity_structure(self, *streams: SyncStream) -> Any:
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
            function_hash_mode=self.tag_function_hash_mode,
            hash_kwargs=hash_function_kwargs,
        )
        
        # Convert list to hashable representation
        # Handle potentially unhashable elements by converting to string
        try:
            elements_hashable = tuple(self.elements)
        except TypeError:
            # If elements are not hashable, convert to string representation
            elements_hashable = tuple(str(elem) for elem in self.elements)
        
        return (
            self.__class__.__name__,
            self.name,
            elements_hashable,
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
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )

        if self.expected_tag_keys is not None:
            return tuple(self.expected_tag_keys), (self.name,)
        return super().keys(trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: "SyncStream", trigger_run: bool = True
    ) -> bool | None:
        if len(streams) != 0:
            raise ValueError(
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )
        # Claim uniqueness only if the default tag function is used
        if self.tag_function == self.__class__.default_tag_function:
            return True
        # Otherwise, delegate to the base class
        return super().claims_unique_tags(trigger_run=trigger_run)
