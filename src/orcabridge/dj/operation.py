from .stream import DJStream, FixedStreamFromTable
from ..mapper import Join

class JoinDJ(Join, DJStream):
    """
    DataJoint specific Join operation that only works on DJ table streams
    """
    
    def __init__(self, *streams):
        self.streams = streams
        joined_query = streams[0].query
        for next_stream in streams[1:]:
            joined_query = joined_query * next_stream.query
        tables = set()
        for stream in streams:
            tables.update(stream.tables)
        self._tables = tables
        self._query = joined_query

    @property
    def tags(self):
        return self._query.primary_key

    def __iter__(self):
        for row in self._query.fetch(as_dict=True):
            yield {k: row[k] for k in self.tags}, {k: row[k] for k in row if k not in self.tags}

    def __len__(self):
        return self._query.count()

    
