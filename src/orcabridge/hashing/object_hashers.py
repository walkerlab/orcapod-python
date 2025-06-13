from .types import FunctionInfoExtractor
from .core import hash_object


class DefaultObjectHasher:
    """
    Default object hasher that returns the string representation of the object.
    """

    def __init__(self, function_info_extractor: FunctionInfoExtractor | None = None):
        """
        Initializes the hasher with an optional function info extractor.

        Args:
            function_info_extractor (FunctionInfoExtractor | None): Optional extractor for function information. This must be provided if an object containing function information is to be hashed.
        """
        self.function_info_extractor = function_info_extractor

    def hash_to_hex(self, obj: Any):
        pass
