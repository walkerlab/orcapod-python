from typing import Any, Self, cast
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import pyarrow as pa

from collections.abc import Collection


# Converter interfaces using modern generics with ABC
class PythonConverter[T, R](ABC):
    """
    Abstract base class for converters between canonical and Python representation types.
    T: canonical type, R: Python representation type
    """

    def __init__(self):
        # Automatically infer types from inheritance
        self._python_type = self._infer_python_type()

    def _infer_python_type(self) -> type[R]:
        """Infer the Python type from __orig_bases__"""
        for base in getattr(self.__class__, "__orig_bases__", []):
            if hasattr(base, "__origin__") and issubclass(
                base.__origin__, PythonConverter
            ):
                # Get the R type parameter (second argument)
                args = getattr(base, "__args__", ())
                if len(args) >= 2:
                    return args[1]  # R is the second type parameter
        raise RuntimeError(f"Could not infer Python type for {self.__class__.__name__}")

    @abstractmethod
    def to_canonical(self, value: R) -> T:
        """Convert from Python representation to canonical form"""
        pass

    @abstractmethod
    def from_canonical(self, value: T) -> R:
        """Convert from canonical to Python representation form"""
        pass

    @abstractmethod
    def can_handle(self, python_type: type) -> bool: ...

    def get_python_type(self) -> type[R]:
        """Get the Python type this converter converts into (auto-inferred)"""
        return self._python_type


class ArrowConverter[T](ABC):
    """
    Abstract base class for converters between canonical and Arrow representation types.
    T: canonical type
    """

    @abstractmethod
    def to_canonical(self, value: pa.Array) -> list[T]:
        """Convert from Arrow representation to canonical form"""
        pass

    # @abstractmethod
    # def from_canonical_to_arrow_compatible(self, value: T) -> Any:
    #     """Convert from canonical to Arrow-compatible representation"""
    #     pass

    # @abstractmethod
    # def from_arrow_compatible_to_canonical(self, value: Any) -> T:
    #     """Convert from Arrow-compatible representation to canonical form"""
    #     pass

    @abstractmethod
    def from_canonical(self, value: T | Collection[T]) -> pa.Array:
        """Convert from canonical to Arrow representation"""
        pass

    @abstractmethod
    def can_handle(self, arrow_type: pa.DataType) -> bool: ...

    @abstractmethod
    def get_arrow_type(self) -> pa.DataType:
        """Get the Arrow DataType this converter handles"""
        pass


# Canonical types with explicit definitions
@dataclass(frozen=True)
class CanonicalPath:
    """Canonical representation of a file system path"""

    path_str: str
    is_absolute: bool = False

    def __str__(self) -> str:
        return self.path_str

    def __post_init__(self) -> None:
        if not self.path_str:
            raise ValueError("Path string cannot be empty")


@dataclass(frozen=True)
class CanonicalTimestamp:
    """Canonical representation of a timestamp"""

    timestamp: int
    timezone: str = "UTC"

    def __post_init__(self) -> None:
        if self.timestamp < 0:
            raise ValueError("Timestamp cannot be negative")


@dataclass(frozen=True)
class CanonicalURL:
    """Canonical representation of a URL"""

    url: str
    scheme: str
    host: str

    def __post_init__(self) -> None:
        if not self.url.startswith(f"{self.scheme}://"):
            raise ValueError(f"URL must start with {self.scheme}://")


# Python converters for Path
class PathlibPathConverter(PythonConverter[CanonicalPath, Path]):
    """Converter for pathlib.Path objects"""

    def to_canonical(self, value: Path) -> CanonicalPath:
        return CanonicalPath(path_str=str(value), is_absolute=value.is_absolute())

    def from_canonical(self, value: CanonicalPath) -> Path:
        return Path(value.path_str)

    def can_handle(self, python_type: type) -> bool:
        return issubclass(python_type, Path)


# Arrow converters for Path
class ArrowStringPathConverter(ArrowConverter[CanonicalPath]):
    """Converter for Arrow string representation of paths"""

    def to_canonical(self, value: pa.Array) -> list[CanonicalPath]:
        return [
            CanonicalPath(v, is_absolute=Path(v).is_absolute())
            for v in value.to_pylist()
        ]

    def from_canonical(
        self, value: CanonicalPath | Collection[CanonicalPath]
    ) -> pa.Array:
        if isinstance(value, CanonicalPath):
            value = [value]
        return pa.array([v.path_str for v in value], type=pa.large_string())

    def from_canonical_to_arrow_compatible(self, value: CanonicalPath) -> str:
        return value.path_str

    def from_arrow_compatible_to_canonical(self, value: str) -> CanonicalPath:
        return CanonicalPath(path_str=value, is_absolute=Path(value).is_absolute())

    def can_handle(self, arrow_type: pa.DataType) -> bool:
        return arrow_type == pa.large_string()

    def get_arrow_type(self) -> pa.DataType:
        return pa.large_string()


# Enhanced SemanticType with explicit Python and Arrow handling
class SemanticType[T]:
    """
    Represents a semantic type with explicit Python/Arrow converters.

    A SemanticType is a central concept that:
    1. Defines a canonical representation (T) for a domain concept
    2. Manages separate Python and Arrow converters
    3. Provides explicit methods for Python and Arrow operations
    4. Maintains type safety while allowing runtime discovery

    Type parameter T represents the canonical representation type.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        python_converters: Collection[PythonConverter[T, Any]] | None = None,
        arrow_converters: Collection[ArrowConverter[T]] | None = None,
    ):
        self.name = name
        self.description = description

        self._python_type_converters: list[PythonConverter[T, Any]] = []
        self._arrow_type_converters: list[ArrowConverter[T]] = []

        # Default converters
        self._default_python_converter: PythonConverter[T, Any] | None = None
        self._default_arrow_converter: ArrowConverter[T] | None = None

        if python_converters is not None:
            for converter in python_converters:
                self.register_python_converter(
                    converter,
                    set_default=self._default_python_converter is None,
                    force=False,
                )

        if arrow_converters is not None:
            for converter in arrow_converters:
                self.register_arrow_converter(
                    converter,
                    set_default=self._default_arrow_converter is None,
                    force=False,
                )

    def get_default_python_type(self) -> type[T]:
        """Get the default Python type for this semantic type"""
        if self._default_python_converter:
            return self._default_python_converter.get_python_type()
        raise ValueError(
            f"No default Python converter registered for semantic type '{self.name}'"
        )

    def get_default_arrow_type(self) -> pa.DataType:
        """Get the default Arrow DataType for this semantic type"""
        if self._default_arrow_converter:
            return self._default_arrow_converter.get_arrow_type()
        raise ValueError(
            f"No default Arrow converter registered for semantic type '{self.name}'"
        )

    def register_python_converter[R](
        self,
        converter: PythonConverter[T, R],
        set_default: bool = False,
        force: bool = False,
    ):
        """
        Register a Python converter
        """
        if converter not in self._python_type_converters:
            self._python_type_converters.append(converter)

        if set_default:
            if self._default_python_converter is not None and not force:
                raise ValueError(
                    f"Default Python converter already set for semantic type '{self.name}'"
                )
            self._default_python_converter = converter

    def register_arrow_converter(
        self,
        converter: ArrowConverter[T],
        set_default: bool = False,
        force: bool = False,
    ) -> None:
        """Register an Arrow converter"""
        if converter not in self._arrow_type_converters:
            self._arrow_type_converters.append(converter)

        if set_default:
            if self._default_arrow_converter is not None and not force:
                raise ValueError(
                    f"Default Arrow converter already set for semantic type '{self.name}'"
                )
            self._default_arrow_converter = converter

    # Python-specific methods
    def get_python_converter_for_type(
        self, python_type: type
    ) -> PythonConverter[T, Any] | None:
        """Find a Python converter that can handle the given type"""
        for converter in self._python_type_converters:
            if converter.can_handle(python_type):
                return converter
        return None

    def get_arrow_converter_for_type(
        self, arrow_type: pa.DataType
    ) -> ArrowConverter[T] | None:
        """Find an Arrow converter for the given Arrow DataType"""
        for converter in self._arrow_type_converters:
            if converter.can_handle(arrow_type):
                return converter
        return None

    def get_python_converter_with_output_type(
        self, output_type: type
    ) -> PythonConverter[T, Any] | None:
        """Get a Python converter that can handle the specified output type"""
        for converter in self._python_type_converters:
            if issubclass(converter.get_python_type(), output_type):
                return converter
        return None

    def get_arrow_converter_with_output_type(
        self, output_type: pa.DataType
    ) -> ArrowConverter[T] | None:
        for converter in self._arrow_type_converters:
            if output_type == converter.get_arrow_type():
                return converter
        return None

    def supports_python_type(self, python_type: type) -> bool:
        return self.get_python_converter_for_type(python_type) is not None

    def supports_arrow_type(self, arrow_type: pa.DataType) -> bool:
        return self.get_arrow_converter_for_type(arrow_type) is not None

    @property
    def default_python_converter(self) -> PythonConverter[T, Any] | None:
        """Get the default Python converter"""
        return self._default_python_converter

    @property
    def default_arrow_converter(self) -> ArrowConverter[T] | None:
        return self._default_arrow_converter

    def to_canonical_from_python(self, value: Any) -> T:
        """Convert Python value to canonical form"""
        converter = self.get_python_converter_for_type(type(value))
        if not converter:
            raise ValueError(
                f"No Python converter found for {type(value)} in semantic type '{self.name}'"
            )

        return converter.to_canonical(value)

    def from_canonical_to_python(
        self, value: T, target_type: type | None = None
    ) -> Any:
        """Convert from canonical to Python representation"""
        if target_type is None:
            converter = self.default_python_converter
            if not converter:
                raise ValueError(
                    f"No default Python converter for semantic type '{self.name}'"
                )
        else:
            converter = self.get_python_converter_for_type(target_type)
            if not converter:
                raise ValueError(
                    f"No converter found for target type '{target_type}' in semantic type '{self.name}'"
                )

        return converter.from_canonical(value)

    def to_canonical_from_arrow(self, value: pa.Array) -> list[T]:
        """Convert Arrow value to canonical form using explicit Arrow DataType"""
        converter = self.get_arrow_converter_for_type(value.type)
        if not converter:
            raise ValueError(
                f"No Arrow converter found for type '{value.type}' in semantic type '{self.name}'"
            )

        canonical = converter.to_canonical(value)

        return canonical

    def from_canonical_to_arrow(
        self, value: T, target_type: pa.DataType | None = None
    ) -> pa.Array:
        """Convert from canonical to Arrow representation using explicit Arrow DataType"""

        if target_type is None:
            converter = self.default_arrow_converter
            if not converter:
                raise ValueError(
                    f"No default Arrow converter for semantic type '{self.name}'"
                )
        else:
            converter = self.get_arrow_converter_for_type(target_type)
            if not converter:
                raise ValueError(
                    f"No Arrow converter found for target type '{target_type}' in semantic type '{self.name}'"
                )

        return converter.from_canonical(value)

    def get_python_types(self) -> list[type]:
        """Get all supported output Python DataTypes"""
        return [
            converter.get_python_type() for converter in self._python_type_converters
        ]

    def get_arrow_types(self) -> list[pa.DataType]:
        """Get all supported output Arrow DataTypes"""
        return [converter.get_arrow_type() for converter in self._arrow_type_converters]

    # Cross-system conversion methods
    def convert_python_to_arrow(
        self, python_value: Any, arrow_type: pa.DataType | None = None
    ) -> Any:
        """Convert directly from Python to Arrow representation"""
        canonical = self.to_canonical_from_python(python_value)
        return self.from_canonical_to_arrow(canonical, arrow_type)

    def convert_arrow_to_python(
        self, arrow_value, python_type: type | None = None
    ) -> list[Any]:
        """Convert directly from Arrow to Python representation"""
        canonical_values = self.to_canonical_from_arrow(arrow_value)
        return [
            self.from_canonical_to_python(value, target_type=python_type)
            for value in canonical_values
        ]

    def __str__(self) -> str:
        return f"SemanticType(name='{self.name}')"

    def __repr__(self) -> str:
        python_count = len(self._python_type_converters)
        arrow_count = len(self._arrow_type_converters)
        return (
            f"SemanticType(name='{self.name}', "
            f"python_converters={python_count}, "
            f"arrow_converters={arrow_count})"
        )


# Registry with explicit Python and Arrow handling
class SemanticTypeRegistry:
    """Registry that manages SemanticType objects with explicit Python/Arrow operations"""

    def __init__(self, semantic_types: Collection[SemanticType] | None = None):
        self._semantic_type_lut: dict[str, SemanticType] = {}
        self._python_to_semantic_lut: dict[type, SemanticType] = {}
        if semantic_types is not None:
            for semantic_type in semantic_types:
                self.register_semantic_type(semantic_type)

    def register_semantic_type[T](self, semantic_type: SemanticType[T]):
        """Register a semantic type"""
        if semantic_type.name not in self._semantic_type_lut:
            self._semantic_type_lut[semantic_type.name] = semantic_type
        else:
            raise ValueError(
                f"Semantic type {self._semantic_type_lut[semantic_type.name]} is already registered for semantic name {semantic_type.name}"
            )

        python_type = semantic_type.get_default_python_type()
        if python_type is None:
            raise ValueError(
                f"Semantic type {semantic_type.name} does not have a default Python type"
            )
        if python_type in self._python_to_semantic_lut:
            raise ValueError(
                f"Python type {python_type} is already registered for semantic type {self._python_to_semantic_lut[python_type]}"
            )
        self._python_to_semantic_lut[python_type] = semantic_type

    def get_semantic_type_for_python_type(
        self, python_type: type
    ) -> SemanticType | None:
        """Get a semantic type by Python type"""

        # check if it's directly registered
        semantic_type = self._python_to_semantic_lut.get(python_type)
        if semantic_type is None:
            # check if it's a subclass
            for (
                registered_type,
                registered_semantic_type,
            ) in self._python_to_semantic_lut.items():
                if issubclass(python_type, registered_type):
                    return registered_semantic_type
        return semantic_type

    def get_arrow_type_for_semantic_type(
        self, semantic_type_name: str
    ) -> pa.DataType | None:
        """Get the default Arrow DataType for a semantic type by name"""
        semantic_type = self._semantic_type_lut.get(semantic_type_name)
        if semantic_type:
            return semantic_type.get_default_arrow_type()
        return None

    def get_arrow_type_for_python_type(
        self, python_type: type
    ) -> tuple[str | None, pa.DataType] | None:
        """Get the default Arrow DataType for a Python type"""
        semantic_type = self.get_semantic_type_for_python_type(python_type)
        if semantic_type:
            return semantic_type.name, semantic_type.get_default_arrow_type()
        return None

    def from_python_to_arrow(self, python_value: Any) -> tuple[str | None, Any]:
        """Convert a Python value to Arrow-targetting representation using the semantic type registry"""
        semantic_type = self.get_semantic_type_for_python_type(type(python_value))
        if semantic_type:
            return semantic_type.name, semantic_type.convert_python_to_arrow(
                python_value
            )
        return None, python_value

    def get_semantic_type(self, name: str) -> SemanticType | None:
        """Get a semantic type by name"""
        return self._semantic_type_lut.get(name)

    def list_semantic_types(self) -> list[SemanticType]:
        """Get all registered semantic types"""
        return list(self._semantic_type_lut.values())

    def registered_with_semantic_type(self, python_type: type) -> bool:
        """Check if registry has the Python type registered with a semantic type"""
        return python_type in self._python_to_semantic_lut

    def supports_semantic_and_arrow_type(
        self, semantic_type_name: str, arrow_type: pa.DataType
    ) -> bool:
        """Check if registry supports the given semantic type and Arrow DataType combination"""
        semantic_type = self._semantic_type_lut.get(semantic_type_name)
        if not semantic_type:
            return False
        return semantic_type.supports_arrow_type(arrow_type)


# Type-safe wrapper for semantic values
class SemanticValue[T]:
    """Type-safe wrapper for semantic values"""

    def __init__(self, value: T, semantic_type: SemanticType[T]):
        self._value = value
        self._semantic_type = semantic_type

    @property
    def value(self) -> T:
        return self._value

    @property
    def semantic_type(self) -> SemanticType[T]:
        return self._semantic_type

    def to_python(self) -> Any:
        """Convert to Python representation"""
        return self._semantic_type.from_canonical_to_python(self._value)

    def to_python_type(self, python_type: type) -> Any:
        """Convert to Arrow representation using specific Arrow DataType"""
        return self._semantic_type.from_canonical_to_arrow(self._value, python_type)

    def to_arrow(self) -> Any:
        """Convert to Arrow representation using default dtype"""
        return self._semantic_type.from_canonical_to_arrow(self._value)

    def to_arrow_with_type(self, arrow_type: pa.DataType) -> Any:
        """Convert to Arrow representation using specific Arrow DataType"""
        return self._semantic_type.from_canonical_to_arrow(self._value, arrow_type)

    @classmethod
    def from_python(cls, python_value: Any, semantic_type: SemanticType[T]) -> Self:
        """Create from a Python value"""
        canonical = semantic_type.to_canonical_from_python(python_value)
        return cls(canonical, semantic_type)

    @classmethod
    def from_arrow(cls, arrow_value: Any, semantic_type: SemanticType[T]) -> Self:
        """Create from an Arrow value with explicit Arrow DataType"""
        canonical = semantic_type.to_canonical_from_arrow(arrow_value)
        if len(canonical) != 1:
            raise ValueError(
                f"Expected single value from Arrow, got {len(canonical)} values"
            )
        return cls(canonical[0], semantic_type)

    def __str__(self) -> str:
        return f"SemanticValue({self._value}, {self._semantic_type.name})"

    def __repr__(self) -> str:
        return f"SemanticValue(value={self._value!r}, semantic_type={self._semantic_type.name})"


class PythonArrowConverter[T, R]:
    @classmethod
    def from_semantic_type(cls, semantic_type: SemanticType[T]) -> Self:
        """Create a PythonArrowConverter from a SemanticType"""
        python_converter = semantic_type.default_python_converter
        arrow_converter = semantic_type.default_arrow_converter

        if not python_converter or not arrow_converter:
            raise ValueError(
                f"Semantic type '{semantic_type.name}' does not have default converters"
            )

        return cls(python_converter, arrow_converter, semantic_type.name)

    def __init__(
        self,
        python_converter: PythonConverter[T, R],
        arrow_converter: ArrowConverter[T],
        semantic_type_name: str | None = None,
    ):
        self.python_converter = python_converter
        self.arrow_converter = arrow_converter
        self.semantic_type_name = semantic_type_name

    @property
    def python_type(self) -> type[R]:
        """Get the Python type this converter handles"""
        return self.python_converter.get_python_type()

    @property
    def arrow_type(self) -> pa.DataType:
        """Get the Arrow DataType this converter handles"""
        return self.arrow_converter.get_arrow_type()

    def from_python_to_arrow(self, python_value: R | Collection[R]) -> pa.Array:
        """Convert from Python to Arrow representation"""
        if isinstance(python_value, self.python_type):
            python_value = [python_value]
        assert isinstance(python_value, Collection), (
            "Expected a collection of values at this point"
        )
        python_values = cast(Collection[R], python_value)
        canonicals = [self.python_converter.to_canonical(val) for val in python_values]
        return self.arrow_converter.from_canonical(canonicals)

    def from_arrow_to_python(self, arrow_value: pa.Array) -> list[R]:
        """Convert from Arrow to Python representation"""
        canonical = self.arrow_converter.to_canonical(arrow_value)
        return [self.python_converter.from_canonical(value) for value in canonical]
