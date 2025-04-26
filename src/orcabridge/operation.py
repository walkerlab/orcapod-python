from .stream import Stream

class Operation():
    def __call__(self, *stream: Stream) -> Stream:
        raise NotImplementedError("This method should be implemented in subclasses.")
    