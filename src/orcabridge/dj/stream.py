from ..stream import SyncStream


from datajoint.expression import QueryExpression
from datajoint.table import Table
from typing import Collection, Any


class QueryStream(SyncStream):
    """
    DataJoint query-based data stream. Critically, `QueryStream` is backed
    by a DataJoint query, and iterating over it will yield packets equivalent to
    iterating over the query results.
    """

    def __init__(
        self, query: QueryExpression, upstream_tables: Collection[Table]
    ) -> None:
        self.query = query
        self.upstream_tables = upstream_tables

    def __iter__(self):
        for row in self.query.fetch(as_dict=True):
            tag = {k: row[k] for k in self.query.primary_key}
            packet = {k: row[k] for k in row if k not in self.query.primary_key}
            yield tag, packet

    def proj(self, *args, **kwargs) -> "QueryStream":
        """
        Project the table and return a new source.
        """
        return QueryStream(self.query.proj(*args, **kwargs), self.upstream_tables)

    def __and__(self, other: Any) -> "QueryStream":
        """
        Join the table with another table and return a new source.
        """
        # lazy load to avoid circular import
        from ..source import TableSource

        if isinstance(other, TableSource):
            other = other.table
        elif isinstance(other, QueryStream):
            other = other.query
        else:
            raise ValueError(f"Object of type {type(other)} is not supported.")
        return QueryStream(self.query & other, self.upstream_tables)

    def __repr__(self):
        return self.query.__repr__()

    def preview(self, limit=None, width=None):
        return self.query.preview(limit=limit, width=width)

    def _repr_html_(self):
        """:return: HTML to display table in Jupyter notebook."""
        return self.query._repr_html_()


class TableStream(QueryStream):
    """
    DataJoint table-based data stream
    """

    def __init__(self, table: Table) -> None:
        super().__init__(table, [table])
        self.table = table


class TableCachedStream(TableStream):
    """
    A stream that caches the output from another stream into a DJ table
    and then returns the content. Effectively act as a TableStream as
    all returned packets can be found in the table.
    """

    def __init__(self, table: Table, stream: SyncStream) -> None:
        super().__init__(table)
        self.stream = stream

    def __iter__(self):
        for tag, packet in self.stream:
            # cache the packet into the table
            if not self.table & tag:
                self.table.insert1(tag | packet)  # insert the packet into the table
            yield tag, packet
