from ..base import Operation
from .stream import QueryStream


class QueryOperation(Operation):
    """
    A special type of operation that returns and works with
    QueryStreams
    """

    def __call__(self, *streams: QueryStream, **kwargs) -> QueryStream:
        return super().__call__(*streams, **kwargs)
