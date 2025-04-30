from ..stream import SyncStream

import datajoint as dj
from datajoint.expression import QueryExpression
from datajoint.table import Table
from typing import Collection


class QueryStream(SyncStream):
    """
    DataJoint query-based data stream
    """
    def __init__(self, query: QueryExpression, upstream_tables: Collection[Table]) -> None:
        self.query = query
        self.upstream_tables = upstream_tables

    def __iter__(self):
        for row in self.query.fetch(as_dict=True):
            tag = {k: row[k] for k in self.query.primary_key}
            packet = {k: row[k] for k in row if k not in self.query.primary_key}
            yield tag, packet
