from ..stream import SyncStream

import datajoint as dj
from datajoint.expression import QueryExpression
from datajoint.table import Table
from typing import Collection


class QueryStream(SyncStream):
    """
    DataJoint query-based data stream
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
