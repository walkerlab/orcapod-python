from .stream import QueryStream
from ..base import Operation


class QueryOperation(Operation):
    """
    A special type of operation that returns and works with
    QueryStreams
    """

    def __call__(self, *streams: QueryStream) -> QueryStream:
        return super().__call__(*streams)
