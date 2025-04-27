from .stream import SyncStream

class Operation():
    def __call__(self, *streams: SyncStream) -> SyncStream: ...
    