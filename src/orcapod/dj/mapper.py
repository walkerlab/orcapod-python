import warnings
from typing import Optional

from orcapod.core.operators import Join, MapPackets, MapTags, Operator
from .operation import QueryOperation
from .stream import QueryStream


class QueryMapper(QueryOperation, Operator):
    """
    A special type of mapper that returns and works with QueryStreams
    """


def convert_to_query_mapper(operation: Operator) -> QueryMapper:
    """
    Convert a generic mapper to an equivalent, Query mapper
    """

    if isinstance(operation, Join):
        return JoinQuery()
    elif isinstance(operation, MapPackets):
        proj_map = {v: k for k, v in operation.key_map.items()}
        # if drop_unmapped is True, we need to project the keys
        args = [] if operation.drop_unmapped else [...]
        return ProjectQuery(*args, **proj_map)
    elif isinstance(operation, MapTags):
        proj_map = {v: k for k, v in operation.key_map.items()}
        if operation.drop_unmapped:
            warnings.warn("Dropping unmapped tags is not supported in DataJoint")
        return ProjectQuery(..., **proj_map)
    elif isinstance(operation, QueryOperation):
        # if the operation is already a QueryOperation, just return it
        return operation
    else:
        raise ValueError(f"Unknown operation: {operation}")


class JoinQuery(QueryMapper):
    """
    DataJoint specific Join operation that only works on QueryStream
    """

    def forward(self, *streams: QueryStream, project=False) -> QueryStream:
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


class ProjectQuery(QueryMapper):
    """
    Project (rename/remove) tag and packet keys
    """

    def __init__(self, *args, _label: Optional[str] = None, **projection_kwargs):
        super().__init__(label=_label)
        self.projection_args = args
        self.projection_kwargs = projection_kwargs

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            str(self.projection_args),
            str(self.projection_kwargs),
        ) + tuple(streams)

    def forward(self, *streams: QueryStream) -> QueryStream:
        if len(streams) != 1:
            raise ValueError("Project operation requires exactly one stream")

        stream = streams[0]

        # project the query
        projected_query = stream.query.proj(
            *self.projection_args, **self.projection_kwargs
        )

        projected_upstreams = [
            table.proj(*self.projection_args, **self.projection_kwargs)
            for table in stream.upstream_tables
        ]

        return QueryStream(projected_query, projected_upstreams)


class RestrictQuery(QueryMapper):
    """
    Restrict (filter) tag and packet keys
    """

    def __init__(self, *restrictions, label: Optional[str] = None):
        super().__init__(label=label)
        self.restrictions = restrictions

    def identity_structure(self, *streams):
        return (
            self.__class__.__name__,
            str(self.restrictions),
        ) + tuple(streams)

    def forward(self, *streams: QueryStream) -> QueryStream:
        if len(streams) != 1:
            raise ValueError("Restrict operation requires exactly one stream")

        stream = streams[0]

        # restrict the query
        restricted_query = stream.query
        for restriction in self.restrictions:
            restricted_query = restricted_query & restriction

        return QueryStream(restricted_query, stream.upstream_tables)
