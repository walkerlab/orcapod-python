# A collection of utility function that provides a "default" implementation of hashers.
# This is often used as the fallback hasher in the library code.
from orcabridge.hashing.types import CompositeFileHasher
from orcabridge.hashing.file_hashers import PathLikeHasherFactory
from orcabridge.hashing.string_cachers import InMemoryCacher
from orcabridge.hashing.object_hashers import ObjectHasher
from orcabridge.hashing.object_hashers import LegacyObjectHasher
from orcabridge.hashing.function_info_extractors import FunctionInfoExtractorFactory


def get_default_composite_file_hasher(with_cache=True) -> CompositeFileHasher:
    if with_cache:
        # use unlimited caching
        string_cacher = InMemoryCacher(max_size=None)
        return PathLikeHasherFactory.create_cached_composite(string_cacher)
    return PathLikeHasherFactory.create_basic_composite()


def get_default_composite_file_hasher_with_cacher(cacher=None) -> CompositeFileHasher:
    if cacher is None:
        cacher = InMemoryCacher(max_size=None)
    return PathLikeHasherFactory.create_cached_composite(cacher)


def get_default_object_hasher() -> ObjectHasher:
    function_info_extractor = (
        FunctionInfoExtractorFactory.create_function_info_extractor(
            strategy="signature"
        )
    )
    return LegacyObjectHasher(
        char_count=32, function_info_extractor=function_info_extractor
    )
