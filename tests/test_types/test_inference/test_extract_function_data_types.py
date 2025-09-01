"""
Unit tests for the extract_function_typespecs function.

This module tests the function type extraction functionality, covering:
- Type inference from function annotations
- User-provided type overrides
- Various return type scenarios (single, multiple, None)
- Error conditions and edge cases
"""

import pytest
from collections.abc import Collection

from orcapod.utils.types_utils import extract_function_typespecs


class TestExtractFunctionDataTypes:
    """Test cases for extract_function_typespecs function."""

    def test_simple_annotated_function(self):
        """Test function with simple type annotations."""

        def add(x: int, y: int) -> int:
            return x + y

        input_types, output_types = extract_function_typespecs(add, ["result"])

        assert input_types == {"x": int, "y": int}
        assert output_types == {"result": int}

    def test_multiple_return_values_tuple(self):
        """Test function returning multiple values as tuple."""

        def process(data: str) -> tuple[int, str]:
            return len(data), data.upper()

        input_types, output_types = extract_function_typespecs(
            process, ["length", "upper_data"]
        )

        assert input_types == {"data": str}
        assert output_types == {"length": int, "upper_data": str}

    def test_multiple_return_values_list(self):
        """Test function returning multiple values as list."""

        def split_data(data: str) -> tuple[str, str]:
            word1, *words = data.split()
            if len(words) < 1:
                word2 = ""
            else:
                word2 = words[0]
            return word1, word2

        # Note: This tests the case where we have multiple output keys
        # but the return type is list[str] (homogeneous)
        input_types, output_types = extract_function_typespecs(
            split_data, ["first_word", "second_word"]
        )

        assert input_types == {"data": str}
        assert output_types == {"first_word": str, "second_word": str}

    def test_no_return_annotation_multiple_keys(self):
        """Test function with no return annotation and multiple output keys."""

        def mystery_func(x: int):
            return x, str(x)

        with pytest.raises(
            ValueError,
            match="Type for return item 'number' is not specified in output_types",
        ):
            input_types, output_types = extract_function_typespecs(
                mystery_func,
                ["number", "text"],
            )

    def test_input_types_override(self):
        """Test overriding parameter types with input_types."""

        def legacy_func(x, y) -> int:  # No annotations
            return x + y

        input_types, output_types = extract_function_typespecs(
            legacy_func, ["sum"], input_types={"x": int, "y": int}
        )

        assert input_types == {"x": int, "y": int}
        assert output_types == {"sum": int}

    def test_partial_input_types_override(self):
        """Test partial override where some params have annotations."""

        def mixed_func(x: int, y) -> int:  # One annotated, one not
            return x + y

        input_types, output_types = extract_function_typespecs(
            mixed_func, ["sum"], input_types={"y": float}
        )

        assert input_types == {"x": int, "y": float}
        assert output_types == {"sum": int}

    def test_output_types_dict_override(self):
        """Test overriding output types with dict."""

        def mystery_func(x: int) -> str:
            return str(x)

        input_types, output_types = extract_function_typespecs(
            mystery_func, ["result"], output_types={"result": float}
        )

        assert input_types == {"x": int}
        assert output_types == {"result": float}

    def test_output_types_sequence_override(self):
        """Test overriding output types with sequence."""

        def multi_return(data: list) -> tuple[int, float, str]:
            return len(data), sum(data), str(data)

        input_types, output_types = extract_function_typespecs(
            multi_return, ["count", "total", "repr"], output_types=[int, float, str]
        )

        assert input_types == {"data": list}
        assert output_types == {"count": int, "total": float, "repr": str}

    def test_complex_types(self):
        """Test function with complex type annotations."""

        def complex_func(x: str | None, y: int | float) -> tuple[bool, list[str]]:
            return bool(x), [x] if x else []

        input_types, output_types = extract_function_typespecs(
            complex_func, ["is_valid", "items"]
        )

        assert input_types == {"x": str | None, "y": int | float}
        assert output_types == {"is_valid": bool, "items": list[str]}

    def test_none_return_annotation(self):
        """Test function with explicit None return annotation."""

        def side_effect_func(x: int) -> None:
            print(x)

        input_types, output_types = extract_function_typespecs(side_effect_func, [])

        assert input_types == {"x": int}
        assert output_types == {}

    def test_empty_parameters(self):
        """Test function with no parameters."""

        def get_constant() -> int:
            return 42

        input_types, output_types = extract_function_typespecs(get_constant, ["value"])

        assert input_types == {}
        assert output_types == {"value": int}

    # Error condition tests

    def test_missing_parameter_annotation_error(self):
        """Test error when parameter has no annotation and not in input_types."""

        def bad_func(x, y: int):
            return x + y

        with pytest.raises(ValueError, match="Parameter 'x' has no type annotation"):
            extract_function_typespecs(bad_func, ["result"])

    def test_return_annotation_but_no_output_keys_error(self):
        """Test error when function has return annotation but no output keys."""

        def func_with_return(x: int) -> str:
            return str(x)

        with pytest.raises(
            ValueError,
            match="Function has a return type annotation, but no return keys were specified",
        ):
            extract_function_typespecs(func_with_return, [])

    def test_none_return_with_output_keys_error(self):
        """Test error when function returns None but output keys provided."""

        def side_effect_func(x: int) -> None:
            print(x)

        with pytest.raises(
            ValueError,
            match="Function provides explicit return type annotation as None",
        ):
            extract_function_typespecs(side_effect_func, ["result"])

    def test_single_return_multiple_keys_error(self):
        """Test error when single return type but multiple output keys."""

        def single_return(x: int) -> str:
            return str(x)

        with pytest.raises(
            ValueError,
            match="Multiple return keys were specified but return type annotation .* is not a sequence type",
        ):
            extract_function_typespecs(single_return, ["first", "second"])

    def test_unparameterized_sequence_type_error(self):
        """Test error when return type is sequence but not parameterized."""

        def bad_return(x: int) -> tuple:  # tuple without types
            return x, str(x)

        with pytest.raises(
            ValueError, match="is a Sequence type but does not specify item types"
        ):
            extract_function_typespecs(bad_return, ["number", "text"])

    def test_mismatched_return_types_count_error(self):
        """Test error when return type count doesn't match output keys count."""

        def three_returns(x: int) -> tuple[int, str, float]:
            return x, str(x), float(x)

        with pytest.raises(
            ValueError, match="has 3 items, but output_keys has 2 items"
        ):
            extract_function_typespecs(three_returns, ["first", "second"])

    def test_mismatched_output_types_sequence_length_error(self):
        """Test error when output_types sequence length doesn't match output_keys."""

        def func(x: int) -> tuple[int, str]:
            return x, str(x)

        with pytest.raises(
            ValueError,
            match="Output types collection length .* does not match return keys length",
        ):
            extract_function_typespecs(
                func,
                ["first", "second"],
                output_types=[int, str, float],  # Wrong length
            )

    def test_missing_output_type_specification_error(self):
        """Test error when output key not specified and no annotation."""

        def no_return_annotation(x: int):
            return x, str(x)

        with pytest.raises(
            ValueError,
            match="Type for return item 'first' is not specified in output_types",
        ):
            extract_function_typespecs(no_return_annotation, ["first", "second"])

    # Edge cases

    def test_callable_with_args_kwargs(self):
        """Test function with *args and **kwargs."""

        def flexible_func(x: int, *args: str, **kwargs: float) -> bool:
            return True

        input_types, output_types = extract_function_typespecs(
            flexible_func, ["success"]
        )

        assert "x" in input_types
        assert "args" in input_types
        assert "kwargs" in input_types
        assert input_types["x"] is int
        assert output_types == {"success": bool}

    def test_mixed_override_scenarios(self):
        """Test complex scenario with both input and output overrides."""

        def complex_func(a, b: str) -> tuple[int, str]:
            return len(b), b.upper()

        input_types, output_types = extract_function_typespecs(
            complex_func,
            ["length", "upper"],
            input_types={"a": float},
            output_types={"length": int},  # Override only one output
        )

        assert input_types == {"a": float, "b": str}
        assert output_types == {"length": int, "upper": str}

    def test_generic_types(self):
        """Test function with generic type annotations."""

        def generic_func(data: list[int]) -> dict[str, int]:
            return {str(i): i for i in data}

        input_types, output_types = extract_function_typespecs(
            generic_func, ["mapping"]
        )

        assert input_types == {"data": list[int]}
        assert output_types == {"mapping": dict[str, int]}

    def test_sequence_return_type_inference(self):
        """Test that sequence types are properly handled in return annotations."""

        def list_func(
            x: int,
        ) -> tuple[str, int]:  # This should work for multiple outputs
            return str(x), x

        # This tests the sequence detection logic
        input_types, output_types = extract_function_typespecs(
            list_func, ["text", "number"]
        )

        assert input_types == {"x": int}
        assert output_types == {"text": str, "number": int}

    def test_collection_return_type_inference(self):
        """Test Collection type in return annotation."""

        def collection_func(x: int) -> Collection[str]:
            return [str(x)]

        # Single output key with Collection type
        input_types, output_types = extract_function_typespecs(
            collection_func, ["result"]
        )

        assert input_types == {"x": int}
        assert output_types == {"result": Collection[str]}


class TestTypeSpecHandling:
    """Test TypeSpec and type handling edge cases."""

    def test_empty_function(self):
        """Test function with no parameters and no return."""

        def empty_func():
            pass

        input_types, output_types = extract_function_typespecs(empty_func, [])

        assert input_types == {}
        assert output_types == {}

    def test_preserve_annotation_objects(self):
        """Test that complex annotation objects are preserved."""
        from typing import TypeVar, Generic

        T = TypeVar("T")

        class Container(Generic[T]):
            pass

        def generic_container_func(x: Container[int]) -> Container[str]:
            return Container()

        input_types, output_types = extract_function_typespecs(
            generic_container_func, ["result"]
        )

        assert input_types == {"x": Container[int]}
        assert output_types == {"result": Container[str]}

    def test_output_types_dict_partial_override(self):
        """Test partial override with output_types dict."""

        def three_output_func() -> tuple[int, str, float]:
            return 1, "hello", 3.14

        input_types, output_types = extract_function_typespecs(
            three_output_func,
            ["num", "text", "decimal"],
            output_types={"text": bytes},  # Override only middle one
        )

        assert input_types == {}
        assert output_types == {
            "num": int,
            "text": bytes,  # Overridden
            "decimal": float,
        }
