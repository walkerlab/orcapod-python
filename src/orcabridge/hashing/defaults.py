# A collection of utility function that provides a "default" implementation of hashers.
# This is often used as the fallback hasher in the library code.
from orcabridge.hashing.file_hashers import CompositeHasher, HasherFactory
from orcabridge.hashing.string_cachers import InMemoryCacher


def get_default_composite_hasher(with_cache=True) -> CompositeHasher:
    if with_cache:
        # use unlimited caching
        string_cacher = InMemoryCacher(max_size=None)
        return HasherFactory.create_cached_composite(string_cacher)
    return HasherFactory.create_basic_composite()
