from .types import FunctionInfoExtractor
from collections.abc import Callable
from typing import Any, Literal


class FunctionNameExtractor:
    """
    Extractor that only uses the function name for information extraction.
    """

    def extract_function_info(self, func: Callable[..., Any]) -> dict[str, Any]:
        """
        Extracts information from the function based on its name.
        """
        if not callable(func):
            raise TypeError("Provided object is not callable")

        # Use the function's name as the hash
        function_name = func.__name__ if hasattr(func, "__name__") else str(func)
        return {"name": function_name}


class FunctionSignatureExtractor:
    """
    Extractor that uses the function signature for information extraction.
    """

    def extract_function_info(self, func: Callable[..., Any]) -> dict[str, Any]:
        """
        Extracts information from the function based on its signature.
        """
        if not callable(func):
            raise TypeError("Provided object is not callable")

        # Use the function's signature as the hash
        function_signature = str(func.__code__)
        return {"signature": function_signature}


class FunctionInfoExtractorFactory:
    """Factory for creating various extractor combinations."""

    @staticmethod
    def create_function_info_extractor(
        strategy: Literal["name", "signature"] = "signature",
    ) -> FunctionInfoExtractor:
        """Create a basic composite extractor."""
        if strategy == "name":
            return FunctionNameExtractor()
        elif strategy == "signature":
            return FunctionSignatureExtractor()
        else:
            raise ValueError(
                f"Unknown strategy: {strategy}. Use 'name' or 'signature'."
            )
