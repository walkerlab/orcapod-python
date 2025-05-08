from ..stream import SyncStream
import copy


from datajoint.expression import QueryExpression
from datajoint.table import Table
from typing import Collection, Any
import logging

logger = logging.getLogger(__name__)


def query_without_restriction(query: QueryExpression) -> QueryExpression:
    """
    Make a new QueryExpression, copying all attributes except for the restriction
    """
    new_query = copy.copy(query)
    new_query._restriction = None
    new_query._restriction_attributes = None

    return new_query


class QueryStream(SyncStream):
    """
    DataJoint query-based data stream. Critically, `QueryStream` is backed
    by a DataJoint query, and iterating over it will yield packets equivalent to
    iterating over the query results.
    """

    def __init__(self, query: QueryExpression, upstream_tables: Collection[Table]) -> None:
        super().__init__()
        self.query = query
        # remove the restriction from the query
        self.upstream_tables = [query_without_restriction(table) for table in upstream_tables]

    def __iter__(self):
        for row in self.query.fetch(as_dict=True):
            tag = {k: row[k] for k in self.query.primary_key}
            packet = {k: row[k] for k in row if k not in self.query.primary_key}
            yield tag, packet

    def proj(self, *args, **kwargs) -> "QueryStream":
        """
        Project the table and return a new source.
        """
        return QueryStream(
            self.query.proj(*args, **kwargs),
            [table.proj(*args, **kwargs) for table in self.upstream_tables],
        )

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

    def identity_structure(self, *streams) -> Any:
        return (self.__class__.__name__, self.query.make_sql())


class TableStream(QueryStream):
    """
    DataJoint table-based data stream
    """

    def __init__(self, table: Table) -> None:
        if isinstance(table, type):
            table = table()
        super().__init__(table, [table])
        self.table = table


class TableCachedStream(TableStream):
    """
    A stream that caches the output from another stream into a DJ table
    and then returns the content. Effectively act as a TableStream as
    all returned packets can be found in the table.
    """

    def __init__(self, table: Table, stream: SyncStream, batch_size: int = 1, use_skip_duplicates=False) -> None:
        if isinstance(table, type):
            table = table()
        super().__init__(table)
        self.stream = stream
        self.batch_size = batch_size
        self.use_skip_duplicates = use_skip_duplicates

    def __iter__(self):
        logger.info(f"Caching stream into table {self.table.table_name} with batch size {self.batch_size}, use_skip_duplicates={self.use_skip_duplicates}")
        batch = [] 
        batch_count = 0
        for tag, packet in self.stream:
            # cache the packet into the table
            if self.use_skip_duplicates or not self.table & tag:
                # if use_skip_duplicates is True, skip duplicates
                # if not, insert the packet into the table
                batch.append(tag | packet)
                batch_count += 1

            # if batch_size is <= 0, will accumulate all packets into the batch
            # and insert at the very end
            if self.batch_size > 0 and batch_count >= self.batch_size:
                self.table.insert(batch, skip_duplicates=self.use_skip_duplicates)
                logger.debug(f'Inserted batch of size {len(batch)} into table {self.table.table_name}')
                batch = []
                batch_count = 0
            yield tag, packet
        if batch:
            logger.debug(f'Inserted the final batch of size {len(batch)} into table {self.table.table_name}')
            self.table.insert(batch, skip_duplicates=self.use_skip_duplicates)
