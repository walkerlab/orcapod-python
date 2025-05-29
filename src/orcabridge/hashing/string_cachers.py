from orcabridge.hashing.protocols import StringCacher


import threading


class InMemoryCacher(StringCacher):
    """Thread-safe in-memory LRU cache."""

    def __init__(self, max_size: int | None = 1000):
        self.max_size = max_size
        self._cache = {}
        self._access_order = []
        self._lock = threading.RLock()

    def get_cached(self, cache_key: str) -> str | None:
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
