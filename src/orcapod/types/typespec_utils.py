# Library of functions for working with TypeSpecs and for extracting TypeSpecs from a function's signature

from collections.abc import Callable, Collection, Sequence, Mapping
from typing import get_origin, get_args, Any
from orcapod.types.core import TypeSpec
import inspect
import logging


logger = logging.getLogger(__name__)


def verify_against_typespec(packet: dict, typespec: TypeSpec) -> bool:
    """Verify that the dictionary's types match the expected types in the typespec."""
    from beartype.door import is_bearable

    # verify that packet contains no keys not in typespec
    if set(packet.keys()) - set(typespec.keys()):
        logger.warning(
            f"Packet contains keys not in typespec: {set(packet.keys()) - set(typespec.keys())}. "
        )
        return False
    for key, type_info in typespec.items():
        if key not in packet:
            logger.warning(
                f"Key '{key}' not found in packet. Assuming None but this behavior may change in the future"
            )
        if not is_bearable(packet.get(key), type_info):
            logger.warning(
                f"Type mismatch for key '{key}': expected {type_info}, got {packet.get(key)}."
            )
            return False
    return True


# TODO: is_subhint does not handle invariance properly
# so when working with mutable types, we have to make sure to perform deep copy
def check_typespec_compatibility(
    incoming_types: TypeSpec, receiving_types: TypeSpec
) -> bool:
    from beartype.door import is_subhint

    for key, type_info in incoming_types.items():
        if key not in receiving_types:
            logger.warning(f"Key '{key}' not found in parameter types.")
            return False
        if not is_subhint(type_info, receiving_types[key]):
            logger.warning(
                f"Type mismatch for key '{key}': expected {receiving_types[key]}, got {type_info}."
            )
            return False
    return True


def extract_function_typespecs(
    func: Callable,
    output_keys: Collection[str],
    input_types: TypeSpec | None = None,
    output_types: TypeSpec | Sequence[type] | None = None,
) -> tuple[TypeSpec, TypeSpec]:
    """
    Extract input and output data types from a function signature.

    This function analyzes a function's signature to determine the types of its parameters
    and return values. It combines information from type annotations, user-provided type
    specifications, and return key mappings to produce complete type specifications.

    Args:
        func: The function to analyze for type information.
        output_keys: Collection of string keys that will be used to map the function's
            return values. For functions returning a single value, provide a single key.
            For functions returning multiple values (tuple/list), provide keys matching
            the number of return items.
        input_types: Optional mapping of parameter names to their types. If provided,
            these types override any type annotations in the function signature for the
            specified parameters. If a parameter is not in this mapping and has no
            annotation, an error is raised.
        output_types: Optional type specification for return values. Can be either:
            - A dict mapping output keys to types (TypeSpec)
            - A sequence of types that will be mapped to output_keys in order
            These types override any inferred types from the function's return annotation.

    Returns:
        A tuple containing:
        - input_types_dict: Mapping of parameter names to their inferred/specified types
        - output_types_dict: Mapping of output keys to their inferred/specified types

    Raises:
        ValueError: In various scenarios:
            - Parameter has no type annotation and is not in input_types
            - Function has return annotation but no output_keys specified
            - Function has explicit None return but non-empty output_keys provided
            - Multiple output_keys specified but return annotation is not a sequence type
            - Return annotation is a sequence type but doesn't specify item types
            - Number of types in return annotation doesn't match number of output_keys
            - Output types sequence length doesn't match output_keys length
            - Output key not specified in output_types and has no type annotation

    Examples:
        >>> def add(x: int, y: int) -> int:
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(add, ['result'])
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'result': <class 'int'>}

        >>> def process(data: str) -> tuple[int, str]:
        ...     return len(data), data.upper()
        >>> input_types, output_types = extract_function_data_types(
        ...     process, ['length', 'upper_data']
        ... )
        >>> input_types
        {'data': <class 'str'>}
        >>> output_types
        {'length': <class 'int'>, 'upper_data': <class 'str'>}

        >>> def legacy_func(x, y):  # No annotations
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(
        ...     legacy_func, ['sum'],
        ...     input_types={'x': int, 'y': int},
        ...     output_types={'sum': int}
        ... )
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'sum': <class 'int'>}

        >>> def multi_return(data: list) -> tuple[int, float, str]:
        ...     return len(data), sum(data), str(data)
        >>> input_types, output_types = extract_function_data_types(
        ...     multi_return, ['count', 'total', 'repr'],
        ...     output_types=[int, float, str]  # Override with sequence
        ... )
        >>> output_types
        {'count': <class 'int'>, 'total': <class 'float'>, 'repr': <class 'str'>}
    """
    verified_output_types: TypeSpec = {}
    if output_types is not None:
        if isinstance(output_types, dict):
            verified_output_types = output_types
        elif isinstance(output_types, Sequence):
            # If output_types is a collection, convert it to a dict with keys from return_keys
            if len(output_types) != len(output_keys):
                raise ValueError(
                    f"Output types collection length {len(output_types)} does not match return keys length {len(output_keys)}."
                )
            verified_output_types = {k: v for k, v in zip(output_keys, output_types)}

    signature = inspect.signature(func)

    param_info: TypeSpec = {}
    for name, param in signature.parameters.items():
        if input_types and name in input_types:
            param_info[name] = input_types[name]
        else:
            # check if the parameter has annotation
            if param.annotation is not inspect.Signature.empty:
                param_info[name] = param.annotation
            else:
                raise ValueError(
                    f"Parameter '{name}' has no type annotation and is not specified in input_types."
                )

    return_annot = signature.return_annotation
    inferred_output_types: TypeSpec = {}
    if return_annot is not inspect.Signature.empty and return_annot is not None:
        output_item_types = []
        if len(output_keys) == 0:
            raise ValueError(
                "Function has a return type annotation, but no return keys were specified."
            )
        elif len(output_keys) == 1:
            # if only one return key, the entire annotation is inferred as the return type
            output_item_types = [return_annot]
        elif (get_origin(return_annot) or return_annot) in (tuple, list, Sequence):
            if get_origin(return_annot) is None:
                # right type was specified but did not specified the type of items
                raise ValueError(
                    f"Function return type annotation {return_annot} is a Sequence type but does not specify item types."
                )
            output_item_types = get_args(return_annot)
            if len(output_item_types) != len(output_keys):
                raise ValueError(
                    f"Function return type annotation {return_annot} has {len(output_item_types)} items, "
                    f"but output_keys has {len(output_keys)} items."
                )
        else:
            raise ValueError(
                f"Multiple return keys were specified but return type annotation {return_annot} is not a sequence type (list, tuple, Collection)."
            )
        for key, type_annot in zip(output_keys, output_item_types):
            inferred_output_types[key] = type_annot
    elif return_annot is None:
        if len(output_keys) != 0:
            raise ValueError(
                f"Function provides explicit return type annotation as None, but return keys of length {len(output_keys)} were specified."
            )
    else:
        inferred_output_types = {k: inspect.Signature.empty for k in output_keys}

    # TODO: simplify the handling here -- technically all keys should already be in return_types
    for key in output_keys:
        if key in verified_output_types:
            inferred_output_types[key] = verified_output_types[key]
        elif (
            key not in inferred_output_types
            or inferred_output_types[key] is inspect.Signature.empty
        ):
            raise ValueError(
                f"Type for return item '{key}' is not specified in output_types and has no type annotation in function signature."
            )
    return param_info, inferred_output_types


def get_typespec_from_dict(dict: Mapping) -> TypeSpec:
    """
    Returns a TypeSpec for the given dictionary.
    The TypeSpec is a mapping from field name to Python type.
    """
    return {key: type(value) for key, value in dict.items()}


def get_compatible_type(type1: Any, type2: Any) -> Any:
    if type1 is type2:
        return type1
    if issubclass(type1, type2):
        return type2
    if issubclass(type2, type1):
        return type1
    raise TypeError(f"Types {type1} and {type2} are not compatible")


def union_typespecs(left: TypeSpec | None, right: TypeSpec | None) -> TypeSpec | None:
    if left is None:
        return right
    if right is None:
        return left
    # Merge the two TypeSpecs but raise an error if conflicts in types are found
    merged = dict(left)
    for key, right_type in right.items():
        merged[key] = (
            get_compatible_type(merged[key], right_type)
            if key in merged
            else right_type
        )
    return merged


def intersection_typespecs(
    left: TypeSpec | None, right: TypeSpec | None
) -> TypeSpec | None:
    """
    Returns the intersection of two TypeSpecs, only returning keys that are present in both.
    If a key is present in both TypeSpecs, the type must be the same.
    """
    if left is None or right is None:
        return None
    # Find common keys and ensure types match
    common_keys = set(left.keys()).intersection(set(right.keys()))
    intersection = {}
    for key in common_keys:
        try:
            intersection[key] = get_compatible_type(left[key], right[key])
        except TypeError:
            # If types are not compatible, raise an error
            raise TypeError(
                f"Type conflict for key '{key}': {left[key]} vs {right[key]}"
            )

    return intersection
