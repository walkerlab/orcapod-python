
from .stream import SyncStream
class DJStream(SyncStream):
    """
    DataJoint query-backed data streams
    """

    @property
    def query(self):
        return self._query
    
    @property
    def tables(self):
        return self._tables
    

class FixedStreamFromTable(DJStream):
    def __init__(self, table):
        self.table = table
        self._tables = set([table.full_table_name])
        self._query = table() if isinstance(table, type) else table


    @property
    def tags(self):
        return self.table.primary_key

    def __iter__(self):
        for row in self.table.fetch(as_dict=True):
            yield {k: row[k] for k in self.tags}, {k: row[k] for k in row if k not in self.tags}

    def __len__(self):
        return self.table.count()