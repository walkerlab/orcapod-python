"""Hash strategy protocols for dependency injection."""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable
import uuid

from orcabridge.types import Packet, PathLike, PathSet, TypeSpec

import pyarrow as pa


@runtime_checkable
class Identifiable(Protocol):
    """Protocol for objects that can provide an identity structure."""

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.

        Returns:
            Any: A structure representing this object's content.
                 Should be deterministic and include all identity-relevant data.
                 Return None to indicate no custom identity is available.
        """
        pass  # pragma: no cover


class ObjectHasher(ABC):
    """Abstract class for general object hashing."""

    @abstractmethod
    def hash(self, obj: Any) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (Any): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        ...

    def hash_to_hex(self, obj: Any, char_count: int | None = None) -> str:
        hash_bytes = self.hash(obj)
        hex_str = hash_bytes.hex()

        # TODO: clean up this logic, as char_count handling is messy
        if char_count is not None:
            if char_count > len(hex_str):
                raise ValueError(
                    f"Cannot truncate to {char_count} chars, hash only has {len(hex_str)}"
                )
            return hex_str[:char_count]
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
        # Use the hex representation as input to UUID5
        return uuid.uuid5(namespace, self.hash(obj))


@runtime_checkable
class FileHasher(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> str: ...


# Higher-level operations that compose file hashing
@runtime_checkable
class PathSetHasher(Protocol):
    """Protocol for hashing pathsets (files, directories, collections)."""

    def hash_pathset(self, pathset: PathSet) -> str: ...


@runtime_checkable
class SemanticHasher(Protocol):
    pass


@runtime_checkable
class PacketHasher(Protocol):
    """Protocol for hashing packets."""

    def hash_packet(self, packet: Packet) -> str: ...


class ArrowPacketHasher:
    """Protocol for hashing arrow packets."""

    def hash_arrow_packet(self, packet: pa.Table) -> str: ...


@runtime_checkable
class StringCacher(Protocol):
    """Protocol for caching string key value pairs."""

    def get_cached(self, cache_key: str) -> str | None: ...
    def set_cached(self, cache_key: str, value: str) -> None: ...
    def clear_cache(self) -> None: ...


# Combined interface for convenience (optional)
@runtime_checkable
class CompositeFileHasher(FileHasher, PathSetHasher, PacketHasher, Protocol):
    """Combined interface for all file-related hashing operations."""

    pass


# Function hasher protocol
@runtime_checkable
class FunctionInfoExtractor(Protocol):
    """Protocol for extracting function information."""

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_types: TypeSpec | None = None,
        output_types: TypeSpec | None = None,
    ) -> dict[str, Any]: ...
