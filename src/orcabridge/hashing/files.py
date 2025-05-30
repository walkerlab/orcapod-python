from orcabridge.types import PathLike, PathSet, Packet
from typing import Optional
from orcabridge.hashing.core import hash_file, hash_pathset, hash_packet
from orcabridge.hashing.types import FileHasher, StringCacher
import threading


# Completely unnecessary to inherit from FileHasher, but this
# allows for type checking based on ininstance
class DefaultFileHasher(FileHasher):
    """Default implementation for file hashing."""

    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
        char_count: int | None = 32,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size
        self.char_count = char_count

    def hash_file(self, file_path: PathLike) -> str:
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )

    def hash_pathset(self, pathset: PathSet) -> str:
        return hash_pathset(
            pathset,
            algorithm=self.algorithm,
            buffer_size=self.buffer_size,
            char_count=self.char_count,
            file_hasher=self.hash_file,
        )

    def hash_packet(self, packet: Packet) -> str:
        return hash_packet(
            packet,
            algorithm=self.algorithm,
            buffer_size=self.buffer_size,
            char_count=self.char_count,
            pathset_hasher=self.hash_pathset,
        )


class InMemoryCacher(StringCacher):
    """Thread-safe in-memory LRU cache."""

    def __init__(self, max_size: int | None = 1000):
        self.max_size = max_size
        self._cache = {}
        self._access_order = []
        self._lock = threading.RLock()

    def get_cached(self, cache_key: str) -> Optional[str]:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
                self._access_order.append(cache_key)
                return self._cache[cache_key]
            return None

    def set_cached(self, cache_key: str, value: str) -> None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
            elif self.max_size is not None and len(self._cache) >= self.max_size:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]
            self._cache[cache_key] = value
            self._access_order.append(cache_key)

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
            self._access_order.clear()


class CachedFileHasher(FileHasher):
    """FileHasher with caching capabilities."""

    def __init__(
        self,
        file_hasher: FileHasher,
        string_cacher: StringCacher,
        cache_file=True,
        cache_pathset=False,
        cache_packet=False,
    ):
        self.file_hasher = file_hasher
        self.string_cacher = string_cacher
        self.cache_file = cache_file
        self.cache_pathset = cache_pathset
        self.cache_packet = cache_packet

    def hash_file(self, file_path: PathLike) -> str:
        cache_key = f"file:{file_path}"
        if self.cache_file:
            cached_value = self.string_cacher.get_cached(cache_key)
            if cached_value is not None:
                return cached_value
        value = self.file_hasher.hash_file(file_path)
        if self.cache_file:
            # Store the hash in the cache
            self.string_cacher.set_cached(cache_key, value)
        return value
