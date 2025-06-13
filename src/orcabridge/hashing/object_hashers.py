from .types import FunctionInfoExtractor, ObjectHasher
from .core import legacy_hash


class LegacyObjectHasher(ObjectHasher):
    """
    Legacy object hasher that returns the string representation of the object.

    Note that this is "legacy" in the sense that it is not recommended for use in new code.
    It is provided for compatibility with existing code that relies on this behavior.
    Namely, this algorithm makes use of the
    """

    def __init__(
        self,
        char_count: int | None = 32,
        function_info_extractor: FunctionInfoExtractor | None = None,
    ):
        """
        Initializes the hasher with an optional function info extractor.

        Args:
            function_info_extractor (FunctionInfoExtractor | None): Optional extractor for function information. This must be provided if an object containing function information is to be hashed.
        """
        self.char_count = char_count
        self.function_info_extractor = function_info_extractor

    def hash(self, obj: object) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (object): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        return legacy_hash(obj, function_info_extractor=self.function_info_extractor)
