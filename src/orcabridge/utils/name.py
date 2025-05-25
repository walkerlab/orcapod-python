"""
Utility functions for handling names
"""

import re
import inspect
import pickle
import types
import ast


def find_noncolliding_name(name: str, lut: dict) -> str:
    """
    Generate a unique name that does not collide with existing keys in a lookup table (lut).

    If the given name already exists in the lookup table, a numeric suffix is appended
    to the name (e.g., "name_1", "name_2") until a non-colliding name is found.

    Parameters:
        name (str): The base name to check for collisions.
        lut (dict): A dictionary representing the lookup table of existing names.

    Returns:
        str: A unique name that does not collide with any key in the lookup table.

    Example:
        >>> lut = {"name": 1, "name_1": 2}
        >>> find_noncolliding_name("name", lut)
        'name_2'
    """
    if name not in lut:
        return name

    suffix = 1
    while f"{name}_{suffix}" in lut:
        suffix += 1

    return f"{name}_{suffix}"


def pascal_to_snake(name: str) -> str:
    # Convert PascalCase to snake_case
    # if already in snake_case, return as is
    # TODO: replace this crude check with a more robust one
    if "_" in name:
        # put everything into lowercase and return
        return name.lower()
    # Replace uppercase letters with underscore followed by lowercase letter
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_pascal(name: str) -> str:
    # Convert snake_case to PascalCase
    # if already in PascalCase, return as is
    if "_" not in name:
        # capitalize the name and return
        return name.capitalize()
    # Split the string by underscores and capitalize each component
    components = name.split("_")
    return "".join(x.title() for x in components)


# def get_function_signature(func):
#     """
#     Returns a string representation of how the function arguments were defined.
#     Example output: f(a, b, c, d=0, **kwargs)
#     """
#     sig = inspect.signature(func)
#     function_name = func.__name__

#     param_strings = []

#     for name, param in sig.parameters.items():
#         # Handle different parameter kinds
#         if param.kind == param.POSITIONAL_ONLY:
#             formatted_param = f"{name}, /"
#         elif param.kind == param.POSITIONAL_OR_KEYWORD:
#             if param.default is param.empty:
#                 formatted_param = name
#             else:
#                 # Format the default value
#                 default = repr(param.default)
#                 formatted_param = f"{name}={default}"
#         elif param.kind == param.VAR_POSITIONAL:
#             formatted_param = f"*{name}"
#         elif param.kind == param.KEYWORD_ONLY:
#             if param.default is param.empty:
#                 formatted_param = f"*, {name}"
#             else:
#                 default = repr(param.default)
#                 formatted_param = f"{name}={default}"
#         elif param.kind == param.VAR_KEYWORD:
#             formatted_param = f"**{name}"

#         param_strings.append(formatted_param)

#     params_str = ", ".join(param_strings)
#     return f"{function_name}({params_str})"
