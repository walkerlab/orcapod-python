from orcapod.hashing.hash_utils import hash_file
from orcapod.protocols.hashing_protocols import (
    FileContentHasher,
    StringCacher,
)
from orcapod.types import PathLike


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


# class LegacyDefaultFileHasher:
#     def __init__(
#         self,
#         algorithm: str = "sha256",
#         buffer_size: int = 65536,
#     ):
#         self.algorithm = algorithm
#         self.buffer_size = buffer_size

#     def hash_file(self, file_path: PathLike) -> str:
#         return legacy_core.hash_file(
#             file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
#         )


# class LegacyCachedFileHasher:
#     """File hasher with caching."""

#     def __init__(
#         self,
#         file_hasher: LegacyFileHasher,
#         string_cacher: StringCacher,
#     ):
#         self.file_hasher = file_hasher
#         self.string_cacher = string_cacher

#     def hash_file(self, file_path: PathLike) -> str:
#         cache_key = f"file:{file_path}"
#         cached_value = self.string_cacher.get_cached(cache_key)
#         if cached_value is not None:
#             return cached_value

#         value = self.file_hasher.hash_file(file_path)
#         self.string_cacher.set_cached(cache_key, value)
#         return value


# class LegacyDefaultPathsetHasher:
#     """Default pathset hasher that composes file hashing."""

#     def __init__(
#         self,
#         file_hasher: LegacyFileHasher,
#         char_count: int | None = 32,
#     ):
#         self.file_hasher = file_hasher
#         self.char_count = char_count

#     def _hash_file_to_hex(self, file_path: PathLike) -> str:
#         return self.file_hasher.hash_file(file_path)

#     def hash_pathset(self, pathset: PathSet) -> str:
#         """Hash a pathset using the injected file hasher."""
#         return legacy_core.hash_pathset(
#             pathset,
#             char_count=self.char_count,
#             file_hasher=self.file_hasher.hash_file,  # Inject the method
#         )


# class LegacyDefaultPacketHasher:
#     """Default packet hasher that composes pathset hashing."""

#     def __init__(
#         self,
#         pathset_hasher: LegacyPathSetHasher,
#         char_count: int | None = 32,
#         prefix: str = "",
#     ):
#         self.pathset_hasher = pathset_hasher
#         self.char_count = char_count
#         self.prefix = prefix

#     def _hash_pathset_to_hex(self, pathset: PathSet):
#         return self.pathset_hasher.hash_pathset(pathset)

#     def hash_packet(self, packet: PacketLike) -> str:
#         """Hash a packet using the injected pathset hasher."""
#         hash_str = legacy_core.hash_packet(
#             packet,
#             char_count=self.char_count,
#             prefix_algorithm=False,  # Will apply prefix on our own
#             pathset_hasher=self._hash_pathset_to_hex,  # Inject the method
#         )
#         return f"{self.prefix}-{hash_str}" if self.prefix else hash_str


# # Convenience composite implementation
# class LegacyDefaultCompositeFileHasher:
#     """Composite hasher that implements all interfaces."""

#     def __init__(
#         self,
#         file_hasher: LegacyFileHasher,
#         char_count: int | None = 32,
#         packet_prefix: str = "",
#     ):
#         self.file_hasher = file_hasher
#         self.pathset_hasher = LegacyDefaultPathsetHasher(self.file_hasher, char_count)
#         self.packet_hasher = LegacyDefaultPacketHasher(
#             self.pathset_hasher, char_count, packet_prefix
#         )

#     def hash_file(self, file_path: PathLike) -> str:
#         return self.file_hasher.hash_file(file_path)

#     def hash_pathset(self, pathset: PathSet) -> str:
#         return self.pathset_hasher.hash_pathset(pathset)

#     def hash_packet(self, packet: PacketLike) -> str:
#         return self.packet_hasher.hash_packet(packet)


# # Factory for easy construction
# class LegacyPathLikeHasherFactory:
#     """Factory for creating various hasher combinations."""

#     @staticmethod
#     def create_basic_legacy_composite(
#         algorithm: str = "sha256",
#         buffer_size: int = 65536,
#         char_count: int | None = 32,
#     ) -> LegacyCompositeFileHasher:
#         """Create a basic composite hasher."""
#         file_hasher = LegacyDefaultFileHasher(algorithm, buffer_size)
#         # use algorithm as the prefix for the packet hasher
#         return LegacyDefaultCompositeFileHasher(
#             file_hasher, char_count, packet_prefix=algorithm
#         )

#     @staticmethod
#     def create_cached_legacy_composite(
#         string_cacher: StringCacher,
#         algorithm: str = "sha256",
#         buffer_size: int = 65536,
#         char_count: int | None = 32,
#     ) -> LegacyCompositeFileHasher:
#         """Create a composite hasher with file caching."""
#         basic_file_hasher = LegacyDefaultFileHasher(algorithm, buffer_size)
#         cached_file_hasher = LegacyCachedFileHasher(basic_file_hasher, string_cacher)
#         return LegacyDefaultCompositeFileHasher(
#             cached_file_hasher, char_count, packet_prefix=algorithm
#         )

#     @staticmethod
#     def create_legacy_file_hasher(
#         string_cacher: StringCacher | None = None,
#         algorithm: str = "sha256",
#         buffer_size: int = 65536,
#     ) -> LegacyFileHasher:
#         """Create just a file hasher, optionally with caching."""
#         default_hasher = LegacyDefaultFileHasher(algorithm, buffer_size)
#         if string_cacher is None:
#             return default_hasher
#         else:
#             return LegacyCachedFileHasher(default_hasher, string_cacher)

#     @staticmethod
#     def create_file_hasher(
#         string_cacher: StringCacher | None = None,
#         algorithm: str = "sha256",
#         buffer_size: int = 65536,
#     ) -> FileContentHasher:
#         """Create just a file hasher, optionally with caching."""
#         basic_hasher = BasicFileHasher(algorithm, buffer_size)
#         if string_cacher is None:
#             return basic_hasher
#         else:
#             return CachedFileHasher(basic_hasher, string_cacher)
