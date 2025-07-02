# A collection of utility function that provides a "default" implementation of hashers.
# This is often used as the fallback hasher in the library code.
from orcapod.hashing.types import (
    LegacyCompositeFileHasher,
    ArrowHasher,
    StringCacher,
)
from orcapod.hashing.file_hashers import LegacyPathLikeHasherFactory
from orcapod.hashing.string_cachers import InMemoryCacher
from orcapod.hashing.object_hashers import ObjectHasher
from orcapod.hashing.object_hashers import LegacyObjectHasher
from orcapod.hashing.function_info_extractors import FunctionInfoExtractorFactory
from orcapod.hashing.versioned_hashers import (
    get_versioned_semantic_arrow_hasher,
    get_versioned_object_hasher,
)


def get_default_arrow_hasher(
    cache_file_hash: bool | StringCacher = True,
) -> ArrowHasher:
    """
    Get the default Arrow hasher with semantic type support.
    If `cache_file_hash` is True, it uses an in-memory cacher for caching hash values. If a `StringCacher` is provided, it uses that for caching file hashes.
    """
    arrow_hasher = get_versioned_semantic_arrow_hasher()
    if cache_file_hash:
        # use unlimited caching
        if isinstance(cache_file_hash, StringCacher):
            string_cacher = cache_file_hash
        else:
            string_cacher = InMemoryCacher(max_size=None)

        arrow_hasher.set_cacher("path", string_cacher)

    return arrow_hasher


def get_default_object_hasher() -> ObjectHasher:
    object_hasher = get_versioned_object_hasher()
    return object_hasher


def get_legacy_object_hasher() -> ObjectHasher:
    function_info_extractor = (
        FunctionInfoExtractorFactory.create_function_info_extractor(
            strategy="signature"
        )
    )
    return LegacyObjectHasher(function_info_extractor=function_info_extractor)


def get_default_composite_file_hasher(with_cache=True) -> LegacyCompositeFileHasher:
    if with_cache:
        # use unlimited caching
        string_cacher = InMemoryCacher(max_size=None)
        return LegacyPathLikeHasherFactory.create_cached_legacy_composite(string_cacher)
    return LegacyPathLikeHasherFactory.create_basic_legacy_composite()


def get_default_composite_file_hasher_with_cacher(
    cacher=None,
) -> LegacyCompositeFileHasher:
    if cacher is None:
        cacher = InMemoryCacher(max_size=None)
    return LegacyPathLikeHasherFactory.create_cached_legacy_composite(cacher)
