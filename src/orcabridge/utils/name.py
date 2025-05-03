"""
Utility functions for handling names
"""

import re
import inspect
import hashlib
import pickle
import types
import ast


def pascal_to_snake(name: str) -> str:
    # Convert PascalCase to snake_case
    # if already in snake_case, return as is
    # TODO: replace this crude check with a more robust one
    if "_" in name:
        return name
    # Replace uppercase letters with underscore followed by lowercase letter
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_pascal(name: str) -> str:
    # Convert snake_case to PascalCase
    # if already in PascalCase, return as is
    if "_" not in name:
        return name
    # Split the string by underscores and capitalize each component
    components = name.split("_")
    return "".join(x.title() for x in components)


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


def get_function_signature(func):
    """
    Returns a string representation of how the function arguments were defined.
    Example output: f(a, b, c, d=0, **kwargs)
    """
    sig = inspect.signature(func)
    function_name = func.__name__

    param_strings = []

    for name, param in sig.parameters.items():
        # Handle different parameter kinds
        if param.kind == param.POSITIONAL_ONLY:
            formatted_param = f"{name}, /"
        elif param.kind == param.POSITIONAL_OR_KEYWORD:
            if param.default is param.empty:
                formatted_param = name
            else:
                # Format the default value
                default = repr(param.default)
                formatted_param = f"{name}={default}"
        elif param.kind == param.VAR_POSITIONAL:
            formatted_param = f"*{name}"
        elif param.kind == param.KEYWORD_ONLY:
            if param.default is param.empty:
                formatted_param = f"*, {name}"
            else:
                default = repr(param.default)
                formatted_param = f"{name}={default}"
        elif param.kind == param.VAR_KEYWORD:
            formatted_param = f"**{name}"

        param_strings.append(formatted_param)

    params_str = ", ".join(param_strings)
    return f"{function_name}({params_str})"
