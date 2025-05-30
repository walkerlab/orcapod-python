# # a function to hash a dictionary of key value pairs into uuid
# from collections.abc import Collection, Mapping
# import hashlib
# import uuid
# from uuid import UUID
# from typing import Any, Dict, Optional, Union
# import inspect
# import json

# import hashlib

# # arbitrary depth of nested dictionaries
# T = Dict[str, Union[str, "T"]]


# # TODO: implement proper recursive hashing


# def hash_dict(d: T) -> UUID:
#     # Convert the dictionary to a string representation
#     dict_str = str(sorted(d.items()))

#     # Create a hash of the string representation
#     hash_object = hashlib.sha256(dict_str.encode("utf-8"))

#     # Convert the hash to a UUID
#     hash_uuid = uuid.UUID(hash_object.hexdigest())

#     return hash_uuid


# def stable_hash(s):
#     """Create a stable hash that returns the same integer value across sessions."""
#     # Convert input to bytes if it's not already
#     if not isinstance(s, bytes):
#         s = str(s).encode("utf-8")

#     hash_hex = hashlib.sha256(s).hexdigest()
#     return int(hash_hex[:16], 16)


# def hash_function(function, function_hash_mode: str = "content", hasher_kwargs=None) -> str:
#     """
#     Hash a function based on its content, signature, or name.
#
#     Args:
#         function: The function to hash
#         function_hash_mode: The mode of hashing ('content', 'signature', 'name')
#         function_name: Optional name for the function (if not provided, uses function's __name__)

#     Returns:
#         A string representing the hash of the function
#     """
#     if hasher_kwargs is None:
#         hasher_kwargs = {}

#     if function_hash_mode == "content":
#         function_hash = function_content_hash(function, **hasher_kwargs)
#     elif function_hash_mode == "signature":
#         function_hash = stable_hash(get_function_signature(function, **hasher_kwargs))
#     elif function_hash_mode == "name":
#         function_hash = stable_hash(function.__name__)

#     return function_hash


# def function_content_hash(
#     func, exclude_name=False, exclude_module=False, exclude_declaration=False, return_components=False
# ):
#     """
#     Compute a hash based on the function's source code, name, module, and closure variables.
#     """
#     components = []

#     # Add function name
#     if not exclude_name:
#         components.append(f"name:{func.__name__}")

#     # Add module
#     if not exclude_module:
#         components.append(f"module:{func.__module__}")

#     # Get the function's source code
#     try:
#         source = inspect.getsource(func)
#         # Clean up the source code
#         source = source.strip()
#         # Remove the function definition line
#         if exclude_declaration:
#             # find the line that starts with def and remove it
#             # TODO: consider dealing with more sophisticated cases like decorators
#             source = "\n".join(line for line in source.split("\n") if not line.startswith("def "))
#         components.append(f"source:{source}")
#     except (IOError, TypeError):
#         # If we can't get the source (e.g., built-in function), use the function's string representation
#         components.append(f"repr:{repr(func)}")

#     # Add closure variables if any
#     if func.__closure__:
#         closure_values = []
#         for cell in func.__closure__:
#             # Try to get a stable representation of the cell content
#             try:
#                 # For simple immutable objects
#                 if isinstance(cell.cell_contents, (int, float, str, bool, type(None))):
#                     closure_values.append(repr(cell.cell_contents))
#                 # For other objects, we'll use their string representation
#                 else:
#                     closure_values.append(str(cell.cell_contents))
#             except:
#                 # If we can't get a stable representation, use the cell's id
#                 closure_values.append(f"cell_id:{id(cell)}")

#         components.append(f"closure:{','.join(closure_values)}")

#     # Add function attributes that affect behavior
#     if hasattr(func, "__defaults__") and func.__defaults__:
#         defaults_str = ",".join(repr(d) for d in func.__defaults__)
#         components.append(f"defaults:{defaults_str}")

#     if hasattr(func, "__kwdefaults__") and func.__kwdefaults__:
#         kwdefaults_str = ",".join(f"{k}={repr(v)}" for k, v in func.__kwdefaults__.items())
#         components.append(f"kwdefaults:{kwdefaults_str}")

#     # Function's code object properties (excluding filename and line numbers)
#     code = func.__code__
#     code_props = {
#         "co_argcount": code.co_argcount,
#         "co_posonlyargcount": getattr(code, "co_posonlyargcount", 0),  # Python 3.8+
#         "co_kwonlyargcount": code.co_kwonlyargcount,
#         "co_nlocals": code.co_nlocals,
#         "co_stacksize": code.co_stacksize,
#         "co_flags": code.co_flags,
#         "co_code": code.co_code,
#         "co_names": code.co_names,
#         "co_varnames": code.co_varnames,
#     }
#     components.append(f"code_properties:{repr(code_props)}")
#     if return_components:
#         return components

#     # Join all components and compute hash
#     combined = "\n".join(components)
#     return hashlib.sha256(combined.encode("utf-8")).hexdigest()


# class HashableMixin:
#     """A mixin that provides content-based hashing functionality."""

#     def identity_structure(self) -> Any:
#         """
#         Return a structure that represents the identity of this object.
#         By default, returns None to indicate that no custom structure is provided.
#         Subclasses should override this method to provide meaningful representations.

#         Returns:
#             None to indicate no custom structure (use default hash)
#         """
#         return None

#     def content_hash(self, char_count: Optional[int] = 16) -> str:
#         """
#         Generate a stable string hash based on the object's content.

#         Returns:
#             str: A hexadecimal digest representing the object's content
#         """
#         # Get the identity structure
#         structure = self.identity_structure()

#         # TODO: consider returning __hash__ based value if structure is None

#         # Generate a hash from the identity structure
#         return self._hash_structure(structure, char_count=char_count)

#     def content_hash_int(self, hexdigits=16) -> int:
#         """
#         Generate a stable integer hash based on the object's content.

#         Returns:
#             int: An integer representing the object's content
#         """
#         return int(self.content_hash(char_count=None)[:hexdigits], 16)

#     def __hash__(self) -> int:
#         """
#         Hash implementation that uses the identity structure if provided,
#         otherwise falls back to the superclass's hash method.

#         Returns:
#             int: A hash value based on either content or identity
#         """
#         # Get the identity structure
#         structure = self.identity_structure()

#         # If no custom structure is provided, use the superclass's hash
#         if structure is None:
#             return super().__hash__()

#         # Generate a hash and convert to integer
#         hash_hex = self._hash_structure(structure, char_count=None)
#         return int(hash_hex[:16], 16)

#     def _hash_structure(self, structure: Any, char_count: Optional[int] = 16) -> str:
#         """
#         Helper method to compute a hash string from a structure.

#         Args:
#             structure: The structure to hash

#         Returns:
#             str: A hexadecimal hash digest of the structure
#         """
#         processed = self._process_structure(structure)
#         json_str = json.dumps(processed, sort_keys=True).encode()
#         return hashlib.sha256(json_str).hexdigest()[:char_count]

#     def _process_structure(self, obj: Any) -> Any:
#         """
#         Recursively process a structure to prepare it for hashing.

#         Args:
#             obj: The object or structure to process

#         Returns:
#             A processed version of the structure with HashableMixin objects replaced by their hashes
#         """
#         # Handle None
#         if obj is None:
#             return "None"

#         # If the object is a HashableMixin, use its content_hash
#         if isinstance(obj, HashableMixin):
#             # Don't call content_hash on self to avoid cycles
#             if obj is self:
#                 # TODO: carefully consider this case
#                 # Use the superclass's hash for self
#                 return str(super(HashableMixin, self).__hash__())
#             return obj.content_hash()

#         # Handle basic types
#         if isinstance(obj, (str, int, float, bool)):
#             return str(obj)

#         # Handle named tuples (which are subclasses of tuple)
#         if hasattr(obj, "_fields") and isinstance(obj, tuple):
#             # For namedtuples, convert to dict and then process
#             return self._process_structure({field: value for field, value in zip(obj._fields, obj)})

#         # Handle mappings (dict-like objects)
#         if isinstance(obj, Mapping):
#             return {str(k): self._process_structure(v) for k, v in sorted(obj.items(), key=lambda x: str(x[0]))}

#         # Handle sets and frozensets specifically
#         if isinstance(obj, (set, frozenset)):
#             # Process each item first, then sort the processed results
#             processed_items = [self._process_structure(item) for item in obj]
#             return sorted(processed_items, key=str)

#         # Handle collections (list-like objects)
#         if isinstance(obj, Collection):
#             return [self._process_structure(item) for item in obj]

#         # For bytes and bytearray, convert to hex representation
#         if isinstance(obj, (bytes, bytearray)):
#             return obj.hex()

#         # For other objects, just use their string representation
#         return str(obj)
