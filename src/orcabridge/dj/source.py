from ..source import Source
from .stream import QueryStream
from datajoint import Table
from typing import Any, Collection, Union



class TableSource(Source):
    """
    A source that reads from a table.
    """

    def __init__(self, table: Union[Table, type[Table]]) -> None:
        # if table is a class, instantiate it for consistency
        if isinstance(table, type):
            table = table()
        self.table = table

    def __call__(self) -> QueryStream:
        """
        Read from the table and return a stream of packets.
        """
        return QueryStream(self.table, [self.table])

    def proj(self, *args, **kwargs) -> 'TableSource':
        """
        Project the table and return a new source.
        """
        return TableSource(self.table.proj(*args, **kwargs))
    
    def __and__(self, other: Any) -> 'TableSource':
        """
        Join the table with another table and return a new source.
        """
        if isinstance(other, TableSource):
            other = other.table
        elif isinstance(other, QueryStream):
            other = other.query
        return TableSource(self.table & other)
    
    def __repr__(self):
        return self.table.__repr__()

    def preview(self, limit=None, width=None):
        return self.table.preview(self, limit=limit, width=width)

    def _repr_html_(self):
        """:return: HTML to display table in Jupyter notebook."""
        return self.table._repr_html_()
    
  