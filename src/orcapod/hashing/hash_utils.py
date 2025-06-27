from typing import Any
from .function_info_extractors import FunctionInfoExtractor
import logging
import json
from uuid import UUID
from pathlib import Path
from collections.abc import Mapping, Collection
import hashlib
import xxhash
import zlib

logger = logging.getLogger(__name__)


def serialize_through_json(processed_obj) -> bytes:
    """
    Create a deterministic string representation of a processed object structure.

    Args:
        processed_obj: The processed object to serialize

    Returns:
        A bytes object ready for hashing
    """
    # TODO: add type check of processed obj
    return json.dumps(processed_obj, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def process_structure(
    obj: Any,
    visited: set[int] | None = None,
    function_info_extractor: FunctionInfoExtractor | None = None,
    force_hash: bool = False,
) -> Any:
    """
    Recursively process a structure to prepare it for hashing.

    Args:
        obj: The object or structure to process
        visited: Set of object ids already visited (to handle circular references)
        function_info_extractor: FunctionInfoExtractor to be used for extracting necessary function representation

    Returns:
        A processed version of the structure suitable for stable hashing
    """
    # Initialize the visited set if this is the top-level call
    if visited is None:
        visited = set()

    # Check for circular references - use object's memory address
    # NOTE: While id() is not stable across sessions, we only use it within a session
    # to detect circular references, not as part of the final hash
    obj_id = id(obj)
    if obj_id in visited:
        logger.debug(
            f"Detected circular reference for object of type {type(obj).__name__}"
        )
        return "CircularRef"  # Don't include the actual id in hash output

    # For objects that could contain circular references, add to visited
    if isinstance(obj, (dict, list, tuple, set)) or not isinstance(
        obj, (str, int, float, bool, type(None))
    ):
        visited.add(obj_id)

    # Handle None
    if obj is None:
        return None

    from .content_identifiable import ContentIdentifiableBase

    if isinstance(obj, ContentIdentifiableBase):
        logger.debug(
            f"Processing ContentHashableBase instance of type {type(obj).__name__}"
        )
        # replace the object with expanded identity structure and re-process
        return process_structure(
            obj.identity_structure(), visited, function_info_extractor
        )

    # Handle basic types
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Handle bytes and bytearray
    if isinstance(obj, (bytes, bytearray)):
        logger.debug(
            f"Converting bytes/bytearray of length {len(obj)} to hex representation"
        )
        return obj.hex()

    # Handle Path objects
    if isinstance(obj, Path):
        logger.debug(f"Converting Path object to string: {obj}")
        return str(obj)

    # Handle UUID objects
    if isinstance(obj, UUID):
        logger.debug(f"Converting UUID to string: {obj}")
        return str(obj)

    # Handle named tuples (which are subclasses of tuple)
    if hasattr(obj, "_fields") and isinstance(obj, tuple):
        logger.debug(f"Processing named tuple of type {type(obj).__name__}")
        # For namedtuples, convert to dict and then process
        d = {field: getattr(obj, field) for field in obj._fields}  # type: ignore
        return process_structure(d, visited, function_info_extractor)

    # Handle mappings (dict-like objects)
    if isinstance(obj, Mapping):
        # Process both keys and values
        processed_items = [
            (
                process_structure(k, visited, function_info_extractor),
                process_structure(v, visited, function_info_extractor),
            )
            for k, v in obj.items()
        ]

        # Sort by the processed keys for deterministic order
        processed_items.sort(key=lambda x: str(x[0]))

        # Create a new dictionary with string keys based on processed keys
        # TODO: consider checking for possibly problematic values in processed_k
        # and issue a warning
        return {
            str(processed_k): processed_v
            for processed_k, processed_v in processed_items
        }

    # Handle sets and frozensets
    if isinstance(obj, (set, frozenset)):
        logger.debug(
            f"Processing set/frozenset of type {type(obj).__name__} with {len(obj)} items"
        )
        # Process each item first, then sort the processed results
        processed_items = [
            process_structure(item, visited, function_info_extractor) for item in obj
        ]
        return sorted(processed_items, key=str)

    # Handle collections (list-like objects)
    if isinstance(obj, Collection):
        logger.debug(
            f"Processing collection of type {type(obj).__name__} with {len(obj)} items"
        )
        return [
            process_structure(item, visited, function_info_extractor) for item in obj
        ]

    # For functions, use the function_content_hash
    if callable(obj) and hasattr(obj, "__code__"):
        logger.debug(f"Processing function: {getattr(obj, '__name__')}")
        if function_info_extractor is not None:
            # Use the extractor to get a stable representation
            function_info = function_info_extractor.extract_function_info(obj)
            logger.debug(f"Extracted function info: {function_info} for {obj.__name__}")

            # simply return the function info as a stable representation
            return function_info
        else:
            raise ValueError(
                f"Function {obj} encountered during processing but FunctionInfoExtractor is missing"
            )

    # handle data types
    if isinstance(obj, type):
        logger.debug(f"Processing class/type: {obj.__name__}")
        return f"type:{obj.__class__.__module__}.{obj.__class__.__name__}"

    # For other objects, attempt to create deterministic representation only if force_hash=True
    class_name = obj.__class__.__name__
    module_name = obj.__class__.__module__
    if force_hash:
        try:
            import re

            logger.debug(
                f"Processing generic object of type {module_name}.{class_name}"
            )

            # Try to get a stable dict representation if possible
            if hasattr(obj, "__dict__"):
                # Sort attributes to ensure stable order
                attrs = sorted(
                    (k, v) for k, v in obj.__dict__.items() if not k.startswith("_")
                )
                # Limit to first 10 attributes to avoid extremely long representations
                if len(attrs) > 10:
                    logger.debug(
                        f"Object has {len(attrs)} attributes, limiting to first 10"
                    )
                    attrs = attrs[:10]
                attr_strs = [f"{k}={type(v).__name__}" for k, v in attrs]
                obj_repr = f"{{{', '.join(attr_strs)}}}"
            else:
                # Get basic repr but remove memory addresses
                logger.debug(
                    "Object has no __dict__, using repr() with memory address removal"
                )
                obj_repr = repr(obj)
                if len(obj_repr) > 1000:
                    logger.debug(
                        f"Object repr is {len(obj_repr)} chars, truncating to 1000"
                    )
                    obj_repr = obj_repr[:1000] + "..."
                # Remove memory addresses which look like '0x7f9a1c2b3d4e'
                obj_repr = re.sub(r" at 0x[0-9a-f]+", " at 0xMEMADDR", obj_repr)

            return f"{module_name}.{class_name}:{obj_repr}"
        except Exception as e:
            # Last resort - use class name only
            logger.warning(f"Failed to process object representation: {e}")
            try:
                return f"object:{obj.__class__.__module__}.{obj.__class__.__name__}"
            except AttributeError:
                logger.error("Could not determine object class, using UnknownObject")
                return "UnknownObject"
    else:
        raise ValueError(
            f"Processing of {obj} of type {module_name}.{class_name} is not supported"
        )


def hash_object(
    obj: Any,
    function_info_extractor: FunctionInfoExtractor | None = None,
) -> bytes:
    # Process the object to handle nested structures and HashableMixin instances
    processed = process_structure(obj, function_info_extractor=function_info_extractor)

    # Serialize the processed structure
    json_str = json.dumps(processed, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    logger.debug(
        f"Successfully serialized {type(obj).__name__} using custom serializer"
    )

    # Create the hash
    return hashlib.sha256(json_str).digest()


def hash_file(file_path, algorithm="sha256", buffer_size=65536) -> bytes:
    """
    Calculate the hash of a file using the specified algorithm.

    Parameters:
        file_path (str): Path to the file to hash
        algorithm (str): Hash algorithm to use - options include:
                         'md5', 'sha1', 'sha256', 'sha512', 'xxh64', 'crc32', 'hash_path'
        buffer_size (int): Size of chunks to read from the file at a time

    Returns:
        str: Hexadecimal digest of the hash
    """
    # Verify the file exists
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"The file {file_path} does not exist")

    # Handle special case for 'hash_path' algorithm
    if algorithm == "hash_path":
        # Hash the name of the file instead of its content
        # This is useful for cases where the file content is well known or
        # not relevant
        hasher = hashlib.sha256()
        hasher.update(file_path.encode("utf-8"))
        return hasher.digest()

    # Handle non-cryptographic hash functions
    if algorithm == "xxh64":
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                hasher.update(data)
        return hasher.digest()

    if algorithm == "crc32":
        crc = 0
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                crc = zlib.crc32(data, crc)
        return (crc & 0xFFFFFFFF).to_bytes(4, byteorder="big")

    # Handle cryptographic hash functions from hashlib
    try:
        hasher = hashlib.new(algorithm)
    except ValueError:
        valid_algorithms = ", ".join(sorted(hashlib.algorithms_available))
        raise ValueError(
            f"Invalid algorithm: {algorithm}. Available algorithms: {valid_algorithms}, xxh64, crc32"
        )

    with open(file_path, "rb") as file:
        while True:
            data = file.read(buffer_size)
            if not data:
                break
            hasher.update(data)

    return hasher.digest()
