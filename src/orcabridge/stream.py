class Stream():
    def __iter__(self):
        raise NotImplementedError("Subclasses must implement __iter__ method")


class SyncStream(Stream):
    """
    A stream that will complete in a fixed amount of time. It is suitable for synchronous operations that
    will have to wait for the stream to finish before proceeding.
    """

    def __init__(self, generator_function):
        self.generator_function = generator_function

    def __iter__(self):
        yield from self.generator_function()
    
    def __len__(self):
        return sum(1 for _ in self)
    
    


