import logging
import json
from pathlib import Path
from collections.abc import Collection, Callable
import hashlib
import xxhash
import zlib
import inspect

logger = logging.getLogger(__name__)


# TODO: extract default char count as config
def combine_hashes(
    *hashes: str,
    order: bool = False,
    prefix_hasher_id: bool = False,
    hex_char_count: int | None = 20,
) -> str:
    """Combine hashes into a single hash string."""

    # Sort for deterministic order regardless of input order
    if order:
        prepared_hashes = sorted(hashes)
    else:
        prepared_hashes = list(hashes)
    combined = "".join(prepared_hashes)
    combined_hash = hashlib.sha256(combined.encode()).hexdigest()
    if hex_char_count is not None:
        combined_hash = combined_hash[:hex_char_count]
    if prefix_hasher_id:
        return "sha256@" + combined_hash
    return combined_hash


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


# def process_structure(
#     obj: Any,
#     visited: set[int] | None = None,
#     object_hasher: ObjectHasher | None = None,
#     function_info_extractor: FunctionInfoExtractor | None = None,
#     compressed: bool = False,
#     force_hash: bool = True,
# ) -> Any:
#     """
#     Recursively process a structure to prepare it for hashing.

#     Args:
#         obj: The object or structure to process
#         visited: Set of object ids already visited (to handle circular references)
#         function_info_extractor: FunctionInfoExtractor to be used for extracting necessary function representation

#     Returns:
#         A processed version of the structure suitable for stable hashing
#     """
#     # Initialize the visited set if this is the top-level call
#     if visited is None:
#         visited = set()
#     else:
#         visited = visited.copy()  # Copy to avoid modifying the original set

#     # Check for circular references - use object's memory address
#     # NOTE: While id() is not stable across sessions, we only use it within a session
#     # to detect circular references, not as part of the final hash
#     obj_id = id(obj)
#     if obj_id in visited:
#         logger.debug(
#             f"Detected circular reference for object of type {type(obj).__name__}"
#         )
#         return "CircularRef"  # Don't include the actual id in hash output

#     # For objects that could contain circular references, add to visited
#     if isinstance(obj, (dict, list, tuple, set)) or not isinstance(
#         obj, (str, int, float, bool, type(None))
#     ):
#         visited.add(obj_id)

#     # Handle None
#     if obj is None:
#         return None

#     # TODO: currently using runtime_checkable on ContentIdentifiable protocol
#     # Re-evaluate this strategy to see if a faster / more robust check could be used
#     if isinstance(obj, ContentIdentifiable):
#         logger.debug(
#             f"Processing ContentHashableBase instance of type {type(obj).__name__}"
#         )
#         if compressed:
#             # if compressed, the content identifiable object is immediately replaced with
#             # its hashed string identity
#             if object_hasher is None:
#                 raise ValueError(
#                     "ObjectHasher must be provided to hash ContentIdentifiable objects with compressed=True"
#                 )
#             return object_hasher.hash_object(obj.identity_structure(), compressed=True)
#         else:
#             # if not compressed, replace the object with expanded identity structure and re-process
#             return process_structure(
#                 obj.identity_structure(),
#                 visited,
#                 object_hasher=object_hasher,
#                 function_info_extractor=function_info_extractor,
#             )

#     # Handle basic types
#     if isinstance(obj, (str, int, float, bool)):
#         return obj

#     # Handle bytes and bytearray
#     if isinstance(obj, (bytes, bytearray)):
#         logger.debug(
#             f"Converting bytes/bytearray of length {len(obj)} to hex representation"
#         )
#         return obj.hex()

#     # Handle Path objects
#     if isinstance(obj, Path):
#         logger.debug(f"Converting Path object to string: {obj}")
#         return str(obj)

#     # Handle UUID objects
#     if isinstance(obj, UUID):
#         logger.debug(f"Converting UUID to string: {obj}")
#         return str(obj)

#     # Handle named tuples (which are subclasses of tuple)
#     if hasattr(obj, "_fields") and isinstance(obj, tuple):
#         logger.debug(f"Processing named tuple of type {type(obj).__name__}")
#         # For namedtuples, convert to dict and then process
#         d = {field: getattr(obj, field) for field in obj._fields}  # type: ignore
#         return process_structure(
#             d,
#             visited,
#             object_hasher=object_hasher,
#             function_info_extractor=function_info_extractor,
#             compressed=compressed,
#         )

#     # Handle mappings (dict-like objects)
#     if isinstance(obj, Mapping):
#         # Process both keys and values
#         processed_items = [
#             (
#                 process_structure(
#                     k,
#                     visited,
#                     object_hasher=object_hasher,
#                     function_info_extractor=function_info_extractor,
#                     compressed=compressed,
#                 ),
#                 process_structure(
#                     v,
#                     visited,
#                     object_hasher=object_hasher,
#                     function_info_extractor=function_info_extractor,
#                     compressed=compressed,
#                 ),
#             )
#             for k, v in obj.items()
#         ]

#         # Sort by the processed keys for deterministic order
#         processed_items.sort(key=lambda x: str(x[0]))

#         # Create a new dictionary with string keys based on processed keys
#         # TODO: consider checking for possibly problematic values in processed_k
#         # and issue a warning
#         return {
#             str(processed_k): processed_v
#             for processed_k, processed_v in processed_items
#         }

#     # Handle sets and frozensets
#     if isinstance(obj, (set, frozenset)):
#         logger.debug(
#             f"Processing set/frozenset of type {type(obj).__name__} with {len(obj)} items"
#         )
#         # Process each item first, then sort the processed results
#         processed_items = [
#             process_structure(
#                 item,
#                 visited,
#                 object_hasher=object_hasher,
#                 function_info_extractor=function_info_extractor,
#                 compressed=compressed,
#             )
#             for item in obj
#         ]
#         return sorted(processed_items, key=str)

#     # Handle collections (list-like objects)
#     if isinstance(obj, Collection):
#         logger.debug(
#             f"Processing collection of type {type(obj).__name__} with {len(obj)} items"
#         )
#         return [
#             process_structure(
#                 item,
#                 visited,
#                 object_hasher=object_hasher,
#                 function_info_extractor=function_info_extractor,
#                 compressed=compressed,
#             )
#             for item in obj
#         ]

#     # For functions, use the function_content_hash
#     if callable(obj) and hasattr(obj, "__code__"):
#         logger.debug(f"Processing function: {getattr(obj, '__name__')}")
#         if function_info_extractor is not None:
#             # Use the extractor to get a stable representation
#             function_info = function_info_extractor.extract_function_info(obj)
#             logger.debug(f"Extracted function info: {function_info} for {obj.__name__}")

#             # simply return the function info as a stable representation
#             return function_info
#         else:
#             raise ValueError(
#                 f"Function {obj} encountered during processing but FunctionInfoExtractor is missing"
#             )

#     # handle data types
#     if isinstance(obj, type):
#         logger.debug(f"Processing class/type: {obj.__name__}")
#         return f"type:{obj.__name__}"

#     # For other objects, attempt to create deterministic representation only if force_hash=True
#     class_name = obj.__class__.__name__
#     module_name = obj.__class__.__module__
#     if force_hash:
#         try:
#             import re

#             logger.debug(
#                 f"Processing generic object of type {module_name}.{class_name}"
#             )

#             # Try to get a stable dict representation if possible
#             if hasattr(obj, "__dict__"):
#                 # Sort attributes to ensure stable order
#                 attrs = sorted(
#                     (k, v) for k, v in obj.__dict__.items() if not k.startswith("_")
#                 )
#                 # Limit to first 10 attributes to avoid extremely long representations
#                 if len(attrs) > 10:
#                     logger.debug(
#                         f"Object has {len(attrs)} attributes, limiting to first 10"
#                     )
#                     attrs = attrs[:10]
#                 attr_strs = [f"{k}={type(v).__name__}" for k, v in attrs]
#                 obj_repr = f"{{{', '.join(attr_strs)}}}"
#             else:
#                 # Get basic repr but remove memory addresses
#                 logger.debug(
#                     "Object has no __dict__, using repr() with memory address removal"
#                 )
#                 obj_repr = repr(obj)
#                 if len(obj_repr) > 1000:
#                     logger.debug(
#                         f"Object repr is {len(obj_repr)} chars, truncating to 1000"
#                     )
#                     obj_repr = obj_repr[:1000] + "..."
#                 # Remove memory addresses which look like '0x7f9a1c2b3d4e'
#                 obj_repr = re.sub(r" at 0x[0-9a-f]+", " at 0xMEMADDR", obj_repr)

#             return f"{module_name}.{class_name}:{obj_repr}"
#         except Exception as e:
#             # Last resort - use class name only
#             logger.warning(f"Failed to process object representation: {e}")
#             try:
#                 return f"object:{obj.__class__.__module__}.{obj.__class__.__name__}"
#             except AttributeError:
#                 logger.error("Could not determine object class, using UnknownObject")
#                 return "UnknownObject"
#     else:
#         raise ValueError(
#             f"Processing of {obj} of type {module_name}.{class_name} is not supported"
#         )


# def hash_object(
#     obj: Any,
#     function_info_extractor: FunctionInfoExtractor | None = None,
#     compressed: bool = False,
# ) -> bytes:
#     # Process the object to handle nested structures and HashableMixin instances
#     processed = process_structure(
#         obj, function_info_extractor=function_info_extractor, compressed=compressed
#     )

#     # Serialize the processed structure
#     json_str = json.dumps(processed, sort_keys=True, separators=(",", ":")).encode(
#         "utf-8"
#     )
#     logger.debug(
#         f"Successfully serialized {type(obj).__name__} using custom serializer"
#     )

#     # Create the hash
#     return hashlib.sha256(json_str).digest()


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
