# A collection of utility function that provides a "default" implementation of hashers.
# This is often used as the fallback hasher in the library code.
from orcabridge.hashing.protocols import FileHasher
from orcabridge.hashing.file_hashers import DefaultFileHasher, CachedFileHasher
from orcabridge.hashing.string_cachers import InMemoryCacher


def get_default_file_hasher(with_cache=True) -> FileHasher:
    file_hasher = DefaultFileHasher()
    if with_cache:
        # use unlimited caching
        string_cacher = InMemoryCacher(max_size=None)
        file_hasher = CachedFileHasher(file_hasher, string_cacher)
    return file_hasher
