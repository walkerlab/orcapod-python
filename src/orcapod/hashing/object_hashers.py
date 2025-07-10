from orcapod.protocols.hashing_protocols import FunctionInfoExtractor, ObjectHasher
from orcapod.hashing import legacy_core
from orcapod.hashing import hash_utils
from typing import Any
import uuid
from abc import ABC, abstractmethod


class ObjectHasherBase(ABC):
    @abstractmethod
    def hash(self, obj: object) -> bytes: ...

    @abstractmethod
    def get_hasher_id(self) -> str: ...

    def hash_to_hex(
        self, obj: Any, char_count: int | None = None, prefix_hasher_id: bool = False
    ) -> str:
        hash_bytes = self.hash(obj)
        hex_str = hash_bytes.hex()

        # TODO: clean up this logic, as char_count handling is messy
        if char_count is not None:
            if char_count > len(hex_str):
                raise ValueError(
                    f"Cannot truncate to {char_count} chars, hash only has {len(hex_str)}"
                )
            hex_str = hex_str[:char_count]
        if prefix_hasher_id:
            hex_str = self.get_hasher_id() + "@" + hex_str
        return hex_str

    def hash_to_int(self, obj: Any, hexdigits: int = 16) -> int:
        """
        Hash an object to an integer.

        Args:
            obj (Any): The object to hash.
            hexdigits (int): Number of hexadecimal digits to use for the hash.

        Returns:
            int: The integer representation of the hash.
        """
        hex_hash = self.hash_to_hex(obj, char_count=hexdigits)
        return int(hex_hash, 16)

    def hash_to_uuid(
        self, obj: Any, namespace: uuid.UUID = uuid.NAMESPACE_OID
    ) -> uuid.UUID:
        """Convert hash to proper UUID5."""
        return uuid.uuid5(namespace, self.hash(obj))


class BasicObjectHasher(ObjectHasherBase):
    """
    Default object hasher used throughout the codebase.
    """

    def __init__(
        self,
        hasher_id: str,
        function_info_extractor: FunctionInfoExtractor | None = None,
    ):
        self._hasher_id = hasher_id
        self.function_info_extractor = function_info_extractor

    def get_hasher_id(self) -> str:
        return self._hasher_id

    def hash(self, obj: object) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (object): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        return hash_utils.hash_object(
            obj, function_info_extractor=self.function_info_extractor
        )


class LegacyObjectHasher(ObjectHasherBase):
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

    def get_hasher_id(self) -> str:
        """
        Returns a unique identifier/name assigned to the hasher
        """
        return "legacy_object_hasher"

    def hash(self, obj: object) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (object): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        return legacy_core.legacy_hash(
            obj, function_info_extractor=self.function_info_extractor
        )
