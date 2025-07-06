from orcapod.core.base import Kernel
from .stream import QueryStream


class QueryOperation(Kernel):
    """
    A special type of operation that returns and works with
    QueryStreams
    """

    def __call__(self, *streams: QueryStream, **kwargs) -> QueryStream:
        return super().__call__(*streams, **kwargs)
