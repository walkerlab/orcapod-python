class Operation():
    def __call__(self, *streams: 'SyncStream') -> 'SyncStream': ...

from .stream import SyncStream