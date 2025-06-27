from polars import Object
from .types import FunctionInfoExtractor, ObjectHasher
from .legacy_core import legacy_hash
from .hash_utils import hash_object


class DefaultObjectHasher(ObjectHasher):
    """
    Default object hasher used throughout the codebase.
    """

    def __init__(
        self,
        function_info_extractor: FunctionInfoExtractor | None = None,
    ):
        self.function_info_extractor = function_info_extractor

    def hash(self, obj: object) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (object): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        return hash_object(obj, function_info_extractor=self.function_info_extractor)


class LegacyObjectHasher(ObjectHasher):
    """
    Legacy object hasher that returns the string representation of the object.

    Note that this is "legacy" in the sense that it is not recommended for use in new code.
    It is provided for compatibility with existing code that relies on this behavior.
    Namely, this algorithm makes use of the
    """

    def __init__(
        self,
        function_info_extractor: FunctionInfoExtractor | None = None,
    ):
        """
        Initializes the hasher with an optional function info extractor.

        Args:
            function_info_extractor (FunctionInfoExtractor | None): Optional extractor for function information. This must be provided if an object containing function information is to be hashed.
        """
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
