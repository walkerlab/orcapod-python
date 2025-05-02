from .stream import QueryStream
from ..mapper import Mapper


class JoinQuery(Mapper):
    """
    DataJoint specific Join operation that only works on DJ table streams
    """

    def __call__(self, *streams: QueryStream, project=False) -> QueryStream:
        if len(streams) < 2:
            raise ValueError("Join operation requires at least two streams")

        if not all(isinstance(s, QueryStream) for s in streams):
            raise ValueError("All streams must be QueryStreams")

        # join the tables
        joined_query = None
        upstream_tables = set()
        for stream in streams:
            next_query = stream.query.proj() if project else stream.query
            if joined_query is None:
                joined_query = next_query
            else:
                joined_query = joined_query * stream.query
            upstream_tables.update(stream.upstream_tables)

        return QueryStream(joined_query, upstream_tables)


class ProjectQuery(Mapper):
    """
    Project (rename/remove) tag and packet keys
    """

    def __init__(self, *args, **projection_kwargs):
        self.projection_args = args
        self.projection_kwargs = projection_kwargs

    def __call__(self, *streams: QueryStream) -> QueryStream:
        if len(streams) != 1:
            raise ValueError("Project operation requires exactly one stream")

        stream = streams[0]

        # project the query
        projected_query = stream.query.proj(
            *self.projection_args, **self.projection_kwargs
        )

        return QueryStream(projected_query, stream.upstream_tables)


class RestrictQuery(Mapper):
    """
    Restrict (filter) tag and packet keys
    """

    def __init__(self, *restrictions):
        self.restrictions = restrictions

    def __call__(self, *streams: QueryStream) -> QueryStream:
        if len(streams) != 1:
            raise ValueError("Restrict operation requires exactly one stream")

        stream = streams[0]

        # restrict the query
        restricted_query = stream.query
        for restriction in self.restrictions:
            restricted_query = restricted_query & restriction

        return QueryStream(restricted_query, stream.upstream_tables)
