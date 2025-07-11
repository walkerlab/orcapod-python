import hashlib
import inspect
import json
import logging
import zlib
from orcapod.protocols.hashing_protocols import FunctionInfoExtractor
from functools import partial
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Literal,
    Mapping,
    Optional,
    Set,
    TypeVar,
    Union,
)
from uuid import UUID


import xxhash

from orcapod.types import PathSet, Packet, PacketLike
from orcapod.utils.name import find_noncolliding_name

WARN_NONE_IDENTITY = False
"""
Stable Hashing Library
======================

A library for creating stable, content-based hashes that remain consistent across Python sessions,
suitable for arbitrarily nested data structures and custom objects via HashableMixin.
"""


# Configure logging with __name__ for proper hierarchy
logger = logging.getLogger(__name__)

# Type for recursive dictionary structures
T = TypeVar("T")
NestedDict = Dict[
    str, Union[str, int, float, bool, None, "NestedDict", list, tuple, set]
]


def configure_logging(level=logging.INFO, enable_console=True, log_file=None):
    """
    Optional helper to configure logging for this library.

    Users can choose to use this or configure logging themselves.

    Args:
        level: The logging level (default: INFO)
        enable_console: Whether to log to the console (default: True)
        log_file: Path to a log file (default: None)
    """
    lib_logger = logging.getLogger(__name__)
    lib_logger.setLevel(level)

    # Create a formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Add console handler if requested
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        lib_logger.addHandler(console_handler)

    # Add file handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        lib_logger.addHandler(file_handler)

    lib_logger.debug("Logging configured for stable hash library")
    return lib_logger


def serialize_for_hashing(processed_obj):
    """
    Create a deterministic string representation of a processed object structure.

    This function aims to be more stable than json.dumps() by implementing
    a custom serialization approach for the specific needs of hashing.

    Args:
        processed_obj: The processed object to serialize

    Returns:
        A bytes object ready for hashing
    """
    if processed_obj is None:
        return b"null"

    if isinstance(processed_obj, bool):
        return b"true" if processed_obj else b"false"

    if isinstance(processed_obj, (int, float)):
        return str(processed_obj).encode("utf-8")

    if isinstance(processed_obj, str):
        # Escape quotes and backslashes to ensure consistent representation
        escaped = processed_obj.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'.encode("utf-8")

    if isinstance(processed_obj, list):
        items = [serialize_for_hashing(item) for item in processed_obj]
        return b"[" + b",".join(items) + b"]"

    if isinstance(processed_obj, dict):
        # Sort keys for deterministic order
        sorted_items = sorted(processed_obj.items(), key=lambda x: str(x[0]))
        serialized_items = [
            serialize_for_hashing(k) + b":" + serialize_for_hashing(v)
            for k, v in sorted_items
        ]
        return b"{" + b",".join(serialized_items) + b"}"

    # Fallback for any other type - should not happen after _process_structure
    logger.warning(
        f"Unhandled type in _serialize_for_hashing: {type(processed_obj).__name__}. "
        "Using str() representation as fallback, which may not be stable."
    )
    return str(processed_obj).encode("utf-8")


class HashableMixin:
    """
    A mixin that provides content-based hashing functionality.

    To use this mixin:
    1. Inherit from HashableMixin in your class
    2. Override identity_structure() to return a representation of your object's content
    3. Use content_hash(), content_hash_int(), or __hash__() as needed

    Example:
        class MyClass(HashableMixin):
            def __init__(self, name, value):
                self.name = name
                self.value = value

            def identity_structure(self):
                return {'name': self.name, 'value': self.value}
    """

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.

        Override this method in your subclass to provide a stable representation
        of your object's content. The structure should contain all fields that
        determine the object's identity.

        Returns:
            Any: A structure representing this object's content, or None to use default hash
        """
        return None

    def content_hash(self, char_count: Optional[int] = 16) -> str:
        """
        Generate a stable string hash based on the object's content.

        Args:
            char_count: Number of characters to include in the hex digest (None for full hash)

        Returns:
            str: A hexadecimal digest representing the object's content
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the class name
        # We avoid using id() since it's not stable across sessions
        if structure is None:
            if WARN_NONE_IDENTITY:
                logger.warning(
                    f"HashableMixin.content_hash called on {self.__class__.__name__} "
                    "instance that returned identity_structure() of None. "
                    "Using class name as default identity, which may not correctly reflect object uniqueness."
                )
            # Fall back to class name for consistent behavior
            return f"HashableMixin-DefaultIdentity-{self.__class__.__name__}"

        # Generate a hash from the identity structure
        logger.debug(
            f"Generating content hash for {self.__class__.__name__} using identity structure"
        )
        return hash_to_hex(structure, char_count=char_count)

    def content_hash_int(self, hexdigits: int = 16) -> int:
        """
        Generate a stable integer hash based on the object's content.

        Args:
            hexdigits: Number of hex digits to use for the integer conversion

        Returns:
            int: An integer representing the object's content
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the class name
        # We avoid using id() since it's not stable across sessions
        if structure is None:
            if WARN_NONE_IDENTITY:
                logger.warning(
                    f"HashableMixin.content_hash_int called on {self.__class__.__name__} "
                    "instance that returned identity_structure() of None. "
                    "Using class name as default identity, which may not correctly reflect object uniqueness."
                )
            # Use the same default identity as content_hash for consistency
            default_identity = (
                f"HashableMixin-DefaultIdentity-{self.__class__.__name__}"
            )
            return hash_to_int(default_identity, hexdigits=hexdigits)

        # Generate a hash from the identity structure
        logger.debug(
            f"Generating content hash (int) for {self.__class__.__name__} using identity structure"
        )
        return hash_to_int(structure, hexdigits=hexdigits)

    def content_hash_uuid(self) -> UUID:
        """
        Generate a stable UUID hash based on the object's content.

        Returns:
            UUID: A UUID representing the object's content
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the class name
        # We avoid using id() since it's not stable across sessions
        if structure is None:
            if WARN_NONE_IDENTITY:
                logger.warning(
                    f"HashableMixin.content_hash_uuid called on {self.__class__.__name__} "
                    "instance without identity_structure() implementation. "
                    "Using class name as default identity, which may not correctly reflect object uniqueness."
                )
            # Use the same default identity as content_hash for consistency
            default_identity = (
                f"HashableMixin-DefaultIdentity-{self.__class__.__name__}"
            )
            return hash_to_uuid(default_identity)

        # Generate a hash from the identity structure
        logger.debug(
            f"Generating content hash (UUID) for {self.__class__.__name__} using identity structure"
        )
        return hash_to_uuid(structure)

    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the superclass's hash method.

        Returns:
            int: A hash value based on either content or identity
        """
        # Get the identity structure
        structure = self.identity_structure()

        # If no custom structure is provided, use the superclass's hash
        if structure is None:
            logger.warning(
                f"HashableMixin.__hash__ called on {self.__class__.__name__} "
                "instance without identity_structure() implementation. "
                "Falling back to super().__hash__() which is not stable across sessions."
            )
            return super().__hash__()

        # Generate a hash and convert to integer
        logger.debug(
            f"Generating hash for {self.__class__.__name__} using identity structure"
        )
        return hash_to_int(structure)


# Core hashing functions that serve as the unified interface


def legacy_hash(
    obj: Any, function_info_extractor: FunctionInfoExtractor | None = None
) -> bytes:
    # Process the object to handle nested structures and HashableMixin instances
    processed = process_structure(obj, function_info_extractor=function_info_extractor)

    # Serialize the processed structure
    try:
        # Use custom serialization for maximum stability
        json_str = serialize_for_hashing(processed)
        logger.debug(
            f"Successfully serialized {type(obj).__name__} using custom serializer"
        )
    except Exception as e:
        # Fall back to string representation if serialization fails
        logger.warning(
            f"Custom serialization failed for {type(obj).__name__}, "
            f"falling back to string representation. Error: {e}"
        )
        try:
            # Try standard JSON first
            json_str = json.dumps(processed, sort_keys=True).encode("utf-8")
            logger.info("Successfully used standard JSON serialization as fallback")
        except (TypeError, ValueError) as json_err:
            # If JSON also fails, use simple string representation
            logger.warning(
                f"JSON serialization also failed: {json_err}. "
                "Using basic string representation as last resort."
            )
            json_str = str(processed).encode("utf-8")

    # Create the hash
    return hashlib.sha256(json_str).digest()


def hash_to_hex(
    obj: Any,
    char_count: int | None = 32,
    function_info_extractor: FunctionInfoExtractor | None = None,
) -> str:
    """
    Create a stable hex hash of any object that remains consistent across Python sessions.

    Args:
        obj: The object to hash - can be a primitive type, nested data structure, or
             HashableMixin instance
        char_count: Number of hex characters to return (None for full hash)

    Returns:
        A hex string hash
    """

    # Create the hash
    hash_hex = legacy_hash(obj, function_info_extractor=function_info_extractor).hex()

    # Return the requested number of characters
    if char_count is not None:
        logger.debug(f"Using char_count: {char_count}")
        if char_count > len(hash_hex):
            raise ValueError(
                f"Cannot truncate to {char_count} chars, hash only has {len(hash_hex)}"
            )
        return hash_hex[:char_count]
    return hash_hex


def hash_to_int(
    obj: Any,
    hexdigits: int = 16,
    function_info_extractor: FunctionInfoExtractor | None = None,
) -> int:
    """
    Convert any object to a stable integer hash that remains consistent across Python sessions.

    Args:
        obj: The object to hash
        hexdigits: Number of hex digits to use for the integer conversion

    Returns:
        An integer hash
    """
    hash_hex = hash_to_hex(
        obj, char_count=hexdigits, function_info_extractor=function_info_extractor
    )
    return int(hash_hex, 16)


def hash_to_uuid(
    obj: Any, function_info_extractor: FunctionInfoExtractor | None = None
) -> UUID:
    """
    Convert any object to a stable UUID hash that remains consistent across Python sessions.

    Args:
        obj: The object to hash

    Returns:
        A UUID hash
    """
    hash_hex = hash_to_hex(
        obj, char_count=32, function_info_extractor=function_info_extractor
    )
    # TODO: update this to use UUID5 with a namespace on hash bytes output instead
    return UUID(hash_hex)


# Helper function for processing nested structures
def process_structure(
    obj: Any,
    visited: Optional[Set[int]] = None,
    function_info_extractor: FunctionInfoExtractor | None = None,
) -> Any:
    """
    Recursively process a structure to prepare it for hashing.

    Args:
        obj: The object or structure to process
        visited: Set of object ids already visited (to handle circular references)

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

    # If the object is a HashableMixin, use its content_hash
    if isinstance(obj, HashableMixin):
        logger.debug(f"Processing HashableMixin instance of type {type(obj).__name__}")
        return obj.content_hash()

    from .content_identifiable import ContentIdentifiableBase

    if isinstance(obj, ContentIdentifiableBase):
        logger.debug(
            f"Processing ContentHashableBase instance of type {type(obj).__name__}"
        )
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
    if isinstance(obj, Collection) and not isinstance(obj, str):
        logger.debug(
            f"Processing collection of type {type(obj).__name__} with {len(obj)} items"
        )
        return [
            process_structure(item, visited, function_info_extractor) for item in obj
        ]

    # For functions, use the function_content_hash
    if callable(obj) and hasattr(obj, "__code__"):
        logger.debug(f"Processing function: {obj.__name__}")
        if function_info_extractor is not None:
            # Use the extractor to get a stable representation
            function_info = function_info_extractor.extract_function_info(obj)
            logger.debug(f"Extracted function info: {function_info} for {obj.__name__}")

            # simply return the function info as a stable representation
            return function_info
        else:
            # Default to using legacy function content hash
            return function_content_hash(obj)

    # For other objects, create a deterministic representation
    try:
        import re

        class_name = obj.__class__.__name__
        module_name = obj.__class__.__module__

        logger.debug(f"Processing generic object of type {module_name}.{class_name}")

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

        return f"{module_name}.{class_name}-{obj_repr}"
    except Exception as e:
        # Last resort - use class name only
        logger.warning(f"Failed to process object representation: {e}")
        try:
            return f"Object-{obj.__class__.__module__}.{obj.__class__.__name__}"
        except AttributeError:
            logger.error("Could not determine object class, using UnknownObject")
            return "UnknownObject"


# Function hashing utilities


# Legacy compatibility functions


def hash_dict(d: NestedDict) -> UUID:
    """
    Hash a dictionary with stable results across sessions.

    Args:
        d: The dictionary to hash (can be arbitrarily nested)

    Returns:
        A UUID hash of the dictionary
    """
    return hash_to_uuid(d)


def stable_hash(s: Any) -> int:
    """
    Create a stable hash that returns the same integer value across sessions.

    Args:
        s: The object to hash

    Returns:
        An integer hash
    """
    return hash_to_int(s)


# Hashing of packets and PathSet


class PathSetHasher:
    def __init__(self, char_count=32):
        self.char_count = char_count

    def hash_pathset(self, pathset: PathSet) -> str:
        if isinstance(pathset, str) or isinstance(pathset, PathLike):
            pathset = Path(pathset)
            if not pathset.exists():
                raise FileNotFoundError(f"Path {pathset} does not exist")
            if pathset.is_dir():
                # iterate over all entries in the directory include subdirectory (single step)
                hash_dict = {}
                for entry in pathset.iterdir():
                    file_name = find_noncolliding_name(entry.name, hash_dict)
                    hash_dict[file_name] = self.hash_pathset(entry)
                return hash_to_hex(hash_dict, char_count=self.char_count)
            else:
                # it's a file, hash it directly
                return hash_file(pathset)

        if isinstance(pathset, Collection):
            hash_dict = {}
            for path in pathset:
                # TODO: consider handling of None value
                if path is None:
                    raise NotImplementedError(
                        "Case of PathSet containing None is not supported yet"
                    )
                file_name = find_noncolliding_name(Path(path).name, hash_dict)
                hash_dict[file_name] = self.hash_pathset(path)
            return hash_to_hex(hash_dict, char_count=self.char_count)

        raise ValueError(f"PathSet of type {type(pathset)} is not supported")

    def hash_file(self, filepath) -> str: ...

    def id(self) -> str: ...


def hash_packet_with_psh(
    packet: Packet, algo: PathSetHasher, prefix_algorithm: bool = True
) -> str:
    """
    Generate a hash for a packet based on its content.

    Args:
        packet: The packet to hash
        algorithm: The algorithm to use for hashing
        prefix_algorithm: Whether to prefix the hash with the algorithm name

    Returns:
        A hexadecimal digest of the packet's content
    """
    hash_results = {}
    for key, pathset in packet.items():
        # TODO: fix pathset handling
        hash_results[key] = algo.hash_pathset(pathset)  # type: ignore

    packet_hash = hash_to_hex(hash_results)

    if prefix_algorithm:
        # Prefix the hash with the algorithm name
        packet_hash = f"{algo.id()}-{packet_hash}"

    return packet_hash


def hash_packet(
    packet: PacketLike,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    char_count: Optional[int] = 32,
    prefix_algorithm: bool = True,
    pathset_hasher: Callable[..., str] | None = None,
) -> str:
    """
    Generate a hash for a packet based on its content.

    Args:
        packet: The packet to hash

    Returns:
        A hexadecimal digest of the packet's content
    """
    if pathset_hasher is None:
        pathset_hasher = partial(
            hash_pathset,
            algorithm=algorithm,
            buffer_size=buffer_size,
            char_count=char_count,
        )

    hash_results = {}
    for key, pathset in packet.items():
        # TODO: fix Pathset handling
        hash_results[key] = pathset_hasher(pathset)  # type: ignore

    packet_hash = hash_to_hex(hash_results, char_count=char_count)

    if prefix_algorithm:
        # Prefix the hash with the algorithm name
        packet_hash = f"{algorithm}-{packet_hash}"

    return packet_hash


def hash_pathset(
    pathset: PathSet,
    algorithm="sha256",
    buffer_size=65536,
    char_count: int | None = 32,
    file_hasher: Callable[..., str] | None = None,
) -> str:
    """
    Generate hash of the pathset based primarily on the content of the files.
    If the pathset is a collection of files or a directory, the name of the file
    will be included in the hash calculation.

    Currently only support hashing of Pathset if Pathset points to a single file.
    """
    if file_hasher is None:
        file_hasher = partial(hash_file, algorithm=algorithm, buffer_size=buffer_size)

    if isinstance(pathset, str) or isinstance(pathset, PathLike):
        pathset = Path(pathset)
        if not pathset.exists():
            raise FileNotFoundError(f"Path {pathset} does not exist")
        if pathset.is_dir():
            # iterate over all entries in the directory include subdirectory (single step)
            hash_dict = {}
            for entry in pathset.iterdir():
                file_name = find_noncolliding_name(entry.name, hash_dict)
                hash_dict[file_name] = hash_pathset(
                    entry,
                    algorithm=algorithm,
                    buffer_size=buffer_size,
                    char_count=char_count,
                    file_hasher=file_hasher,
                )
            return hash_to_hex(hash_dict, char_count=char_count)
        else:
            # it's a file, hash it directly
            return file_hasher(pathset)

    if isinstance(pathset, Collection):
        hash_dict = {}
        for path in pathset:
            if path is None:
                raise NotImplementedError(
                    "Case of PathSet containing None is not supported yet"
                )
            file_name = find_noncolliding_name(Path(path).name, hash_dict)
            hash_dict[file_name] = hash_pathset(
                path,
                algorithm=algorithm,
                buffer_size=buffer_size,
                char_count=char_count,
                file_hasher=file_hasher,
            )
        return hash_to_hex(hash_dict, char_count=char_count)


def hash_file(file_path, algorithm="sha256", buffer_size=65536) -> str:
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
        return hash_to_hex(file_path)

    # Handle non-cryptographic hash functions
    if algorithm == "xxh64":
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                hasher.update(data)
        return hasher.hexdigest()

    if algorithm == "crc32":
        crc = 0
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                crc = zlib.crc32(data, crc)
        return format(crc & 0xFFFFFFFF, "08x")  # Convert to hex string

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

    return hasher.hexdigest()


def get_function_signature(
    func: Callable,
    name_override: str | None = None,
    include_defaults: bool = True,
    include_module: bool = True,
    output_names: Collection[str] | None = None,
) -> str:
    """
    Get a stable string representation of a function's signature.

    Args:
        func: The function to process
        include_defaults: Whether to include default values
        include_module: Whether to include the module name

    Returns:
        A string representation of the function signature
    """
    sig = inspect.signature(func)

    # Build the signature string
    parts = {}

    # Add module if requested
    if include_module and hasattr(func, "__module__"):
        parts["module"] = func.__module__

    # Add function name
    parts["name"] = name_override or func.__name__

    # Add parameters
    param_strs = []
    for name, param in sig.parameters.items():
        param_str = str(param)
        if not include_defaults and "=" in param_str:
            param_str = param_str.split("=")[0].strip()
        param_strs.append(param_str)

    parts["params"] = f"({', '.join(param_strs)})"

    # Add return annotation if present
    if sig.return_annotation is not inspect.Signature.empty:
        parts["returns"] = sig.return_annotation

    # TODO: fix return handling
    fn_string = f"{parts['module'] + '.' if 'module' in parts else ''}{parts['name']}{parts['params']}"
    if "returns" in parts:
        fn_string = fn_string + f"-> {str(parts['returns'])}"
    return fn_string


def _is_in_string(line, pos):
    """Helper to check if a position in a line is inside a string literal."""
    # This is a simplified check - would need proper parsing for robust handling
    in_single = False
    in_double = False
    for i in range(pos):
        if line[i] == "'" and not in_double and (i == 0 or line[i - 1] != "\\"):
            in_single = not in_single
        elif line[i] == '"' and not in_single and (i == 0 or line[i - 1] != "\\"):
            in_double = not in_double
    return in_single or in_double


def get_function_components(
    func: Callable,
    name_override: str | None = None,
    include_name: bool = True,
    include_module: bool = True,
    include_declaration: bool = True,
    include_docstring: bool = True,
    include_comments: bool = True,
    preserve_whitespace: bool = True,
    include_annotations: bool = True,
    include_code_properties: bool = True,
) -> list:
    """
    Extract the components of a function that determine its identity for hashing.

    Args:
        func: The function to process
        include_name: Whether to include the function name
        include_module: Whether to include the module name
        include_declaration: Whether to include the function declaration line
        include_docstring: Whether to include the function's docstring
        include_comments: Whether to include comments in the function body
        preserve_whitespace: Whether to preserve original whitespace/indentation
        include_annotations: Whether to include function type annotations
        include_code_properties: Whether to include code object properties

    Returns:
        A list of string components
    """
    components = []

    # Add function name
    if include_name:
        components.append(f"name:{name_override or func.__name__}")

    # Add module
    if include_module and hasattr(func, "__module__"):
        components.append(f"module:{func.__module__}")

    # Get the function's source code
    try:
        source = inspect.getsource(func)

        # Handle whitespace preservation
        if not preserve_whitespace:
            source = inspect.cleandoc(source)

        # Process source code components
        if not include_declaration:
            # Remove function declaration line
            lines = source.split("\n")
            for i, line in enumerate(lines):
                if line.strip().startswith("def "):
                    lines.pop(i)
                    break
            source = "\n".join(lines)

        # Extract and handle docstring separately if needed
        if not include_docstring and func.__doc__:
            # This approach assumes the docstring is properly indented
            # For multi-line docstrings, we need more sophisticated parsing
            doc_str = inspect.getdoc(func)
            if doc_str:
                doc_lines = doc_str.split("\n")
            else:
                doc_lines = []
            doc_pattern = '"""' + "\\n".join(doc_lines) + '"""'
            # Try different quote styles
            if doc_pattern not in source:
                doc_pattern = "'''" + "\\n".join(doc_lines) + "'''"
            source = source.replace(doc_pattern, "")

        # Handle comments (this is more complex and may need a proper parser)
        if not include_comments:
            # This is a simplified approach - would need a proper parser for robust handling
            lines = source.split("\n")
            for i, line in enumerate(lines):
                comment_pos = line.find("#")
                if comment_pos >= 0 and not _is_in_string(line, comment_pos):
                    lines[i] = line[:comment_pos].rstrip()
            source = "\n".join(lines)

        components.append(f"source:{source}")

    except (IOError, TypeError):
        # If source can't be retrieved, fall back to signature
        components.append(f"name:{name_override or func.__name__}")
        try:
            sig = inspect.signature(func)
            components.append(f"signature:{str(sig)}")
        except ValueError:
            components.append("builtin:True")

    # Add function annotations if requested
    if (
        include_annotations
        and hasattr(func, "__annotations__")
        and func.__annotations__
    ):
        sorted_annotations = sorted(func.__annotations__.items())
        annotations_str = ";".join(f"{k}:{v}" for k, v in sorted_annotations)
        components.append(f"annotations:{annotations_str}")

    # Add code object properties if requested
    if include_code_properties:
        code = func.__code__
        stable_code_props = {
            "co_argcount": code.co_argcount,
            "co_kwonlyargcount": getattr(code, "co_kwonlyargcount", 0),
            "co_nlocals": code.co_nlocals,
            "co_varnames": code.co_varnames[: code.co_argcount],
        }
        components.append(f"code_properties:{stable_code_props}")

    return components


def function_content_hash(
    func: Callable,
    include_name: bool = True,
    include_module: bool = True,
    include_declaration: bool = True,
    char_count: Optional[int] = 32,
) -> str:
    """
    Compute a stable hash based on a function's source code and other properties.

    Args:
        func: The function to hash
        include_name: Whether to include the function name in the hash
        include_module: Whether to include the module name in the hash
        include_declaration: Whether to include the function declaration line
        char_count: Number of characters to include in the result

    Returns:
        A hex string hash of the function's content
    """
    logger.debug(f"Generating content hash for function '{func.__name__}'")
    components = get_function_components(
        func,
        include_name=include_name,
        include_module=include_module,
        include_declaration=include_declaration,
    )

    # Join all components and compute hash
    combined = "\n".join(components)
    logger.debug(f"Function components joined, length: {len(combined)} characters")
    return hash_to_hex(combined, char_count=char_count)


def hash_function(
    function: Callable,
    function_hash_mode: Literal["content", "signature", "name"] = "content",
    return_type: Literal["hex", "int", "uuid"] = "hex",
    name_override: Optional[str] = None,
    content_kwargs=None,
    hash_kwargs=None,
) -> Union[str, int, UUID]:
    """
    Hash a function based on specified mode and return type.

    Args:
        function: The function to hash
        function_hash_mode: The mode of hashing ('content', 'signature', or 'name')
        return_type: The format of the hash to return ('hex', 'int', or 'uuid')
        content_kwargs: Additional arguments to pass to the mode-specific function content
            extractors:
            - "content": arguments for get_function_components
            - "signature": arguments for get_function_signature
            - "name": no underlying function used - simply function.__name__ or name_override if provided
        hash_kwargs: Additional arguments for the hashing function that depends on the return type
            - "hex": arguments for hash_to_hex
            - "int": arguments for hash_to_int
            - "uuid": arguments for hash_to_uuid

    Returns:
        A hash of the function in the requested format

    Example:
        >>> def example(x, y=10): return x + y
        >>> hash_function(example)  # Returns content hash as string
        >>> hash_function(example, function_hash_mode="signature")  # Returns signature hash
        >>> hash_function(example, return_type="int")  # Returns content hash as integer
    """
    content_kwargs = content_kwargs or {}
    hash_kwargs = hash_kwargs or {}

    logger.debug(
        f"Hashing function '{function.__name__}' using mode '{function_hash_mode}'"
        + (f" with name override '{name_override}'" if name_override else "")
    )

    if function_hash_mode == "content":
        hash_content = "\n".join(
            get_function_components(
                function, name_override=name_override, **content_kwargs
            )
        )
    elif function_hash_mode == "signature":
        hash_content = get_function_signature(function, **content_kwargs)
    elif function_hash_mode == "name":
        hash_content = name_override or function.__name__
    else:
        err_msg = f"Unknown function_hash_mode: {function_hash_mode}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    # Convert to the requested return type
    if return_type == "hex":
        hash_value = hash_to_hex(hash_content, **hash_kwargs)
    elif return_type == "int":
        hash_value = hash_to_int(hash_content, **hash_kwargs)
    elif return_type == "uuid":
        hash_value = hash_to_uuid(hash_content, **hash_kwargs)
    else:
        err_msg = f"Unknown return_type: {return_type}"
        logger.error(err_msg)
        raise ValueError(err_msg)

    logger.debug(f"Generated hash value as {return_type}: {hash_value}")
    return hash_value
