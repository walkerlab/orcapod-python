"""Hash strategy protocols for dependency injection."""

from collections.abc import Callable
from typing import Protocol, Any, Literal, runtime_checkable
from uuid import UUID
from orcabridge.types import Packet, PathLike, PathSet


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
        ...


@runtime_checkable
class ObjectHasher(Protocol):
    """Protocol for general object hashing."""

    def hash_to_hex(self, obj: Any, char_count: int | None = 32) -> str: ...
    def hash_to_int(self, obj: Any, hexdigits: int = 16) -> int: ...
    def hash_to_uuid(self, obj: Any) -> UUID: ...


@runtime_checkable
class FileHasher(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> str: ...
    def hash_pathset(self, pathset: PathSet) -> str: ...
    def hash_packet(self, packet: Packet) -> str: ...


@runtime_checkable
class FunctionHasher(Protocol):
    """Protocol for function hashing."""

    def hash_function(
        self,
        function: Callable,
        mode: Literal["content", "signature", "name"] = "content",
    ) -> str: ...


@runtime_checkable
class StringCacher(Protocol):
    """Protocol for caching string key value pairs."""

    def get_cached(self, cache_key: str) -> str | None: ...
    def set_cached(self, cache_key: str, value: str) -> None: ...
    def clear_cache(self) -> None: ...
