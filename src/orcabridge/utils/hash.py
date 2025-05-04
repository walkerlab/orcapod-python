# a function to hash a dictionary of key value pairs into uuid
import hashlib
import uuid
from uuid import UUID
from typing import Dict, Union
import inspect

# arbitrary depth of nested dictionaries
T = Dict[str, Union[str, "T"]]


# TODO: implement proper recursive hashing
def hash_dict(d: T) -> UUID:
    # Convert the dictionary to a string representation
    dict_str = str(sorted(d.items()))

    # Create a hash of the string representation
    hash_object = hashlib.md5(dict_str.encode())

    # Convert the hash to a UUID
    hash_uuid = uuid.UUID(hash_object.hexdigest())

    return hash_uuid


import hashlib


def stable_hash(s):
    """Create a stable hash that returns the same integer value across sessions."""
    # Convert input to bytes if it's not already
    if not isinstance(s, bytes):
        s = str(s).encode("utf-8")

    hash_hex = hashlib.sha256(s).hexdigest()
    return int(hash_hex[:16], 16)


def function_content_hash(func):
    """
    Compute a hash based on the function's source code, name, module, and closure variables.
    """
    components = []

    # Add function name
    components.append(f"name:{func.__name__}")

    # Add module
    components.append(f"module:{func.__module__}")

    # Get the function's source code
    try:
        source = inspect.getsource(func)
        # Clean up the source code
        source = source.strip()
        components.append(f"source:{source}")
    except (IOError, TypeError):
        # If we can't get the source (e.g., built-in function), use the function's string representation
        components.append(f"repr:{repr(func)}")

    # Add closure variables if any
    if func.__closure__:
        closure_values = []
        for cell in func.__closure__:
            # Try to get a stable representation of the cell content
            try:
                # For simple immutable objects
                if isinstance(cell.cell_contents, (int, float, str, bool, type(None))):
                    closure_values.append(repr(cell.cell_contents))
                # For other objects, we'll use their string representation
                else:
                    closure_values.append(str(cell.cell_contents))
            except:
                # If we can't get a stable representation, use the cell's id
                closure_values.append(f"cell_id:{id(cell)}")

        components.append(f"closure:{','.join(closure_values)}")

    # Add function attributes that affect behavior
    if hasattr(func, "__defaults__") and func.__defaults__:
        defaults_str = ",".join(repr(d) for d in func.__defaults__)
        components.append(f"defaults:{defaults_str}")

    if hasattr(func, "__kwdefaults__") and func.__kwdefaults__:
        kwdefaults_str = ",".join(
            f"{k}={repr(v)}" for k, v in func.__kwdefaults__.items()
        )
        components.append(f"kwdefaults:{kwdefaults_str}")

    # Function's code object properties (excluding filename and line numbers)
    code = func.__code__
    code_props = {
        "co_argcount": code.co_argcount,
        "co_posonlyargcount": getattr(code, "co_posonlyargcount", 0),  # Python 3.8+
        "co_kwonlyargcount": code.co_kwonlyargcount,
        "co_nlocals": code.co_nlocals,
        "co_stacksize": code.co_stacksize,
        "co_flags": code.co_flags,
        "co_code": code.co_code,
        "co_names": code.co_names,
        "co_varnames": code.co_varnames,
    }
    components.append(f"code_properties:{repr(code_props)}")

    # Join all components and compute hash
    combined = "\n".join(components)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()
