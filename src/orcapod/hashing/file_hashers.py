from orcapod.hashing import legacy_core
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.types import (
    FileContentHasher,
    PathSetHasher,
    StringCacher,
    CompositeFileHasher,
)
from orcapod.types import Packet, PathLike, PathSet


class BasicFileHasher:
    """Basic implementation for file hashing."""

    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_file(self, file_path: PathLike) -> bytes:
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )


class CachedFileHasher:
    """File hasher with caching."""

    def __init__(
        self,
        file_hasher: FileContentHasher,
        string_cacher: StringCacher,
    ):
        self.file_hasher = file_hasher
        self.string_cacher = string_cacher

    def hash_file(self, file_path: PathLike) -> bytes:
        cache_key = f"file:{file_path}"
        cached_value = self.string_cacher.get_cached(cache_key)
        if cached_value is not None:
            return bytes.fromhex(cached_value)

        value = self.file_hasher.hash_file(file_path)
        self.string_cacher.set_cached(cache_key, value.hex())
        return value


# ----------------Legacy implementations for backward compatibility-----------------


class LegacyFileHasher:
    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_file(self, file_path: PathLike) -> bytes:
        return bytes.fromhex(
            legacy_core.hash_file(
                file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
            ),
        )


class LegacyPathsetHasher:
    """Default pathset hasher that composes file hashing."""

    def __init__(
        self,
        file_hasher: FileContentHasher,
        char_count: int | None = 32,
    ):
        self.file_hasher = file_hasher
        self.char_count = char_count

    def _hash_file_to_hex(self, file_path: PathLike) -> str:
        return self.file_hasher.hash_file(file_path).hex()

    def hash_pathset(self, pathset: PathSet) -> bytes:
        """Hash a pathset using the injected file hasher."""
        return bytes.fromhex(
            legacy_core.hash_pathset(
                pathset,
                char_count=self.char_count,
                file_hasher=self._hash_file_to_hex,  # Inject the method
            )
        )


class LegacyPacketHasher:
    """Default packet hasher that composes pathset hashing."""

    def __init__(
        self,
        pathset_hasher: PathSetHasher,
        char_count: int | None = 32,
        prefix: str = "",
    ):
        self.pathset_hasher = pathset_hasher
        self.char_count = char_count
        self.prefix = prefix

    def _hash_pathset_to_hex(self, pathset: PathSet):
        return self.pathset_hasher.hash_pathset(pathset).hex()

    def hash_packet(self, packet: Packet) -> str:
        """Hash a packet using the injected pathset hasher."""
        hash_str = legacy_core.hash_packet(
            packet,
            char_count=self.char_count,
            prefix_algorithm=False,  # Will apply prefix on our own
            pathset_hasher=self._hash_pathset_to_hex,  # Inject the method
        )
        return f"{self.prefix}-{hash_str}" if self.prefix else hash_str


# Convenience composite implementation
class LegacyCompositeFileHasher:
    """Composite hasher that implements all interfaces."""

    def __init__(
        self,
        file_hasher: FileContentHasher,
        char_count: int | None = 32,
        packet_prefix: str = "",
    ):
        self.file_hasher = file_hasher
        self.pathset_hasher = LegacyPathsetHasher(self.file_hasher, char_count)
        self.packet_hasher = LegacyPacketHasher(
            self.pathset_hasher, char_count, packet_prefix
        )

    def hash_file(self, file_path: PathLike) -> bytes:
        return self.file_hasher.hash_file(file_path)

    def hash_pathset(self, pathset: PathSet) -> bytes:
        return self.pathset_hasher.hash_pathset(pathset)

    def hash_packet(self, packet: Packet) -> str:
        return self.packet_hasher.hash_packet(packet)


# Factory for easy construction
class LegacyPathLikeHasherFactory:
    """Factory for creating various hasher combinations."""

    @staticmethod
    def create_basic_legacy_composite(
        algorithm: str = "sha256",
        buffer_size: int = 65536,
        char_count: int | None = 32,
    ) -> CompositeFileHasher:
        """Create a basic composite hasher."""
        file_hasher = LegacyFileHasher(algorithm, buffer_size)
        # use algorithm as the prefix for the packet hasher
        return LegacyCompositeFileHasher(
            file_hasher, char_count, packet_prefix=algorithm
        )

    @staticmethod
    def create_cached_legacy_composite(
        string_cacher: StringCacher,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
        char_count: int | None = 32,
    ) -> CompositeFileHasher:
        """Create a composite hasher with file caching."""
        basic_file_hasher = LegacyFileHasher(algorithm, buffer_size)
        cached_file_hasher = CachedFileHasher(basic_file_hasher, string_cacher)
        return LegacyCompositeFileHasher(
            cached_file_hasher, char_count, packet_prefix=algorithm
        )

    @staticmethod
    def create_file_hasher(
        string_cacher: StringCacher | None = None,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ) -> FileContentHasher:
        """Create just a file hasher, optionally with caching."""
        basic_hasher = BasicFileHasher(algorithm, buffer_size)
        if string_cacher is None:
            return basic_hasher
        else:
            return CachedFileHasher(basic_hasher, string_cacher)
