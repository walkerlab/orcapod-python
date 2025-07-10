"""Hash strategy protocols for dependency injection."""

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable
import uuid

from orcapod.types import TypeSpec, PathLike
import pyarrow as pa


@runtime_checkable
class ContentIdentifiable(Protocol):
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

    def __eq__(self, other: object) -> bool:
        """
        Equality check that compares the identity structures of two objects.

        Args:
            other (object): The object to compare with.

        Returns:
            bool: True if the identity structures are equal, False otherwise.
        """
        ...

    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the default hash.

        Returns:
            int: A hash value based on either content or identity.
        """
        ...


class ObjectHasher(Protocol):
    """Protocol for general object hashing."""

    # TODO: consider more explicitly stating types of objects accepted
    def hash(self, obj: Any) -> bytes:
        """
        Hash an object to a byte representation.

        Args:
            obj (Any): The object to hash.

        Returns:
            bytes: The byte representation of the hash.
        """
        ...

    def get_hasher_id(self) -> str:
        """
        Returns a unique identifier/name assigned to the hasher
        """
        ...

    def hash_to_hex(
        self, obj: Any, char_count: int | None = None, prefix_hasher_id: bool = False
    ) -> str: ...

    def hash_to_int(self, obj: Any, hexdigits: int = 16) -> int:
        """
        Hash an object to an integer.

        Args:
            obj (Any): The object to hash.
            hexdigits (int): Number of hexadecimal digits to use for the hash.

        Returns:
            int: The integer representation of the hash.
        """
        ...

    def hash_to_uuid(
        self, obj: Any, namespace: uuid.UUID = uuid.NAMESPACE_OID
    ) -> uuid.UUID: ...


class FileContentHasher(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> bytes: ...


class ArrowHasher(Protocol):
    """Protocol for hashing arrow packets."""

    def get_hasher_id(self) -> str: ...

    def hash_table(self, table: pa.Table, prefix_hasher_id: bool = True) -> str: ...


class StringCacher(Protocol):
    """Protocol for caching string key value pairs."""

    def get_cached(self, cache_key: str) -> str | None: ...
    def set_cached(self, cache_key: str, value: str) -> None: ...
    def clear_cache(self) -> None: ...


class FunctionInfoExtractor(Protocol):
    """Protocol for extracting function information."""

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: TypeSpec | None = None,
        output_typespec: TypeSpec | None = None,
    ) -> dict[str, Any]: ...


class SemanticTypeHasher(Protocol):
    """Abstract base class for semantic type-specific hashers."""

    def hash_column(
        self,
        column: pa.Array,
    ) -> pa.Array:
        """Hash a column with this semantic type and return the hash bytes."""
        ...

    def set_cacher(self, cacher: StringCacher) -> None:
        """Add a string cacher for caching hash values."""
        ...
