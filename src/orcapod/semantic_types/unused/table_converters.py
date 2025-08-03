"""
Schema system for struct-based semantic types.

This replaces the metadata-based schema handling with explicit struct types
in the Arrow schema itself.
"""

from collections.abc import Mapping
from typing import Any, Protocol, Self
import pyarrow as pa

from orcapod.types import TypeSpec
from .struct_converters import (
    StructConverter,
    SemanticTypeRegistry,
    SemanticStructConverter,
)


class SemanticSchema:
    """Schema that handles semantic types as explicit struct fields."""

    def __init__(self, python_schema: TypeSpec, registry: SemanticTypeRegistry):
        """
        Create a semantic schema.

        Args:
            schema_dict: Mapping of field names to Python types
            registry: Semantic type registry to use
        """
        self.python_schema = dict(python_schema)
        # TODO: integrate with data context system
        self.registry = registry  # or DEFAULT_REGISTRY
        self.converter = SemanticStructConverter(self.registry)

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to Arrow schema with semantic types as structs."""
        fields = []

        for field_name, python_type in self.python_schema.items():
            # Check if this is a semantic type
            converter = self.registry.get_converter_for_python_type(python_type)

            if converter:
                # Use the struct type for semantic types
                arrow_type = converter.arrow_struct_type
            else:
                # Use standard Arrow types for regular types
                arrow_type = self._python_to_arrow_type(python_type)

            fields.append(pa.field(field_name, arrow_type))

        return pa.schema(fields)

    def _python_to_arrow_type(self, python_type: type) -> pa.DataType:
        """Convert Python type to Arrow type for non-semantic types."""
        type_mapping = {
            int: pa.int64(),
            float: pa.float64(),
            str: pa.large_string(),
            bool: pa.bool_(),
            bytes: pa.binary(),
        }

        if python_type in type_mapping:
            return type_mapping[python_type]
        else:
            raise TypeError(f"Unsupported Python type: {python_type}")

    @classmethod
    def from_arrow_schema(
        cls, arrow_schema: pa.Schema, registry: SemanticTypeRegistry
    ) -> "SemanticSchema":
        """Create SemanticSchema from Arrow schema."""
        schema_dict = {}

        for field in arrow_schema:
            if pa.types.is_struct(field.type):
                # Check if this is a semantic struct
                converter = registry.get_converter_for_struct_type(field.type)
                if converter:
                    schema_dict[field.name] = converter.python_type
                else:
                    # Regular struct - not supported yet
                    # TODO: support by constructing typed dictionary
                    raise ValueError(
                        f"Non-semantic struct types not supported: {field.type}"
                    )
            else:
                # Regular Arrow type
                schema_dict[field.name] = cls._arrow_to_python_type(field.type)

        return cls(schema_dict, registry)

    @staticmethod
    def _arrow_to_python_type(arrow_type: pa.DataType) -> type:
        """Convert Arrow type to Python type."""
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return str
        elif pa.types.is_boolean(arrow_type):
            return bool
        elif pa.types.is_binary(arrow_type):
            return bytes
        else:
            raise TypeError(f"Unsupported Arrow type: {arrow_type}")

    def get_semantic_fields(self) -> dict[str, type]:
        """Get fields that are semantic types."""
        semantic_fields = {}
        for field_name, python_type in self.python_schema.items():
            if self.registry.has_python_type(python_type):
                semantic_fields[field_name] = python_type
        return semantic_fields

    def get_regular_fields(self) -> dict[str, type]:
        """Get fields that are regular (non-semantic) types."""
        regular_fields = {}
        for field_name, python_type in self.python_schema.items():
            if not self.registry.has_python_type(python_type):
                regular_fields[field_name] = python_type
        return regular_fields


class SemanticTableConverter(Protocol):
    """Protocol for semantic table converters.

    This defines the interface for converting between Python dicts and Arrow tables
    with semantic types.
    """

    def get_struct_converter(self, field: str) -> StructConverter | None:
        """Get struct converter for a specific field in table."""
        ...

    def python_dict_to_struct_dict(
        self, data_dict: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Convert Python dict to struct dict for semantic fields."""
        ...

    def struct_dict_to_python_dict(
        self, struct_dict: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Convert struct dict back to Python dict for semantic fields."""
        ...

    def python_dict_to_arrow_table(self, data_dict: dict[str, Any]) -> pa.Table:
        """Convert single Python dict to Arrow table."""
        ...

    def python_dicts_to_arrow_table(self, data_dicts: list[dict[str, Any]]) -> pa.Table:
        """Convert list of Python dicts to Arrow table with semantic structs."""
        ...

    def arrow_table_to_python_dicts(self, table: pa.Table) -> list[dict[str, Any]]:
        """Convert Arrow table back to list of Python dicts."""
        ...


class SchemaSemanticTableConverter:
    """Schema-specific semantic converter that pre-resolves semantic type converters for efficiency.

    This converter is optimized for batch processing of data with a consistent schema.
    It pre-resolves all semantic type converters during initialization to avoid
    repeated registry lookups during data conversion.
    """

    def __init__(self, schema: SemanticSchema):
        """
        Create converter for a specific schema.

        Args:
            schema: Semantic schema defining field types and semantic mappings
        """
        self.schema = schema

        # Pre-resolve converters for each semantic field (performance optimization)
        self.field_converters: dict[str, StructConverter] = {}
        self.semantic_fields = set()
        self.regular_fields = set()

        for field_name, python_type in schema.python_schema.items():
            converter = self.schema.registry.get_converter_for_python_type(python_type)
            if converter:
                self.field_converters[field_name] = converter
                self.semantic_fields.add(field_name)
            else:
                self.regular_fields.add(field_name)

    def get_semantic_fields(self) -> tuple[str, ...]:
        """Get names of fields that are semantic types."""
        return tuple(self.field_converters.keys())

    def get_struct_converter_for_field(self, field: str) -> StructConverter | None:
        """Get struct converter for a specific field."""
        return self.field_converters.get(field)

    @classmethod
    def from_python_schema(
        cls, python_schema: TypeSpec, registry: SemanticTypeRegistry
    ) -> Self:
        """Factory method to create converter from schema."""
        return cls(SemanticSchema(python_schema, registry))

    @classmethod
    def from_arrow_schema(
        cls, arrow_schema: "pa.Schema", registry: SemanticTypeRegistry
    ) -> Self:
        return cls(SemanticSchema.from_arrow_schema(arrow_schema, registry))

    def python_dict_to_struct_dict(
        self, data_dict: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Convert Python dict to struct dict for semantic fields."""
        result = dict(data_dict)

        for field_name, converter in self.field_converters.items():
            if field_name in result and result[field_name] is not None:
                result[field_name] = converter.python_to_struct_dict(result[field_name])

        return result

    def struct_dict_to_python_dict(
        self, struct_dict: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Convert struct dict back to Python dict for semantic fields."""
        result = dict(struct_dict)

        for field_name, converter in self.field_converters.items():
            if field_name in result and result[field_name] is not None:
                if isinstance(result[field_name], dict):
                    result[field_name] = converter.struct_dict_to_python(
                        result[field_name]
                    )

        return result

    def python_dicts_to_arrow_table(self, data_dicts: list[dict[str, Any]]) -> pa.Table:
        """Convert list of Python dicts to Arrow table with semantic structs."""
        if not data_dicts:
            raise ValueError("Cannot create table from empty list")

        # Process each field using pre-resolved converters
        arrow_data = {}

        for field_name in self.schema.python_schema.keys():
            values = [d.get(field_name) for d in data_dicts]

            if field_name in self.field_converters:
                # Semantic field - convert to structs using pre-resolved converter
                converter = self.field_converters[field_name]
                struct_dicts = []
                for value in values:
                    if value is not None:
                        struct_dicts.append(converter.python_to_struct_dict(value))
                    else:
                        struct_dicts.append(None)
                arrow_data[field_name] = pa.array(
                    struct_dicts, type=converter.arrow_struct_type
                )
            else:
                # Regular field
                arrow_data[field_name] = pa.array(values)

        return pa.table(arrow_data, schema=self.schema.to_arrow_schema())

    def arrow_table_to_python_dicts(self, table: pa.Table) -> list[dict[str, Any]]:
        """Convert Arrow table back to list of Python dicts."""
        # Convert table to list of dictionaries
        raw_dicts = table.to_pylist()

        # Process each dictionary to convert structs back to Python objects
        python_dicts = []
        for raw_dict in raw_dicts:
            python_dict = {}
            for field_name, value in raw_dict.items():
                if field_name in self.field_converters and isinstance(value, dict):
                    # Convert semantic struct back to Python object using pre-resolved converter
                    converter = self.field_converters[field_name]
                    python_dict[field_name] = converter.struct_dict_to_python(value)
                else:
                    # Regular value
                    python_dict[field_name] = value
            python_dicts.append(python_dict)

        return python_dicts

    def python_dict_to_arrow_table(self, data_dict: dict[str, Any]) -> pa.Table:
        """Convert single Python dict to Arrow table."""
        return self.python_dicts_to_arrow_table([data_dict])


class AutoSemanticTableConverter:
    """General-purpose converter for working with semantic types without pre-defined schema."""

    def __init__(self, registry: SemanticTypeRegistry):
        self.registry = registry
        self.struct_converter = SemanticStructConverter(self.registry)

    def python_dict_to_arrow_table(
        self, data_dict: dict[str, Any], schema: SemanticSchema | None = None
    ) -> pa.Table:
        """Convert dictionary of Python values to Arrow table."""
        if schema is None:
            # Infer schema from data
            schema_dict = {key: type(value) for key, value in data_dict.items()}
            schema = SemanticSchema(schema_dict, self.registry)

        # Use schema-specific converter for efficiency
        converter = SchemaSemanticTableConverter(schema)
        return converter.python_dict_to_arrow_table(data_dict)

    def arrow_table_to_python_dicts(self, table: pa.Table) -> list[dict[str, Any]]:
        """Convert Arrow table back to list of Python dictionaries."""
        # Infer schema from Arrow table
        schema = SemanticSchema.from_arrow_schema(table.schema, self.registry)

        # Use schema-specific converter for efficiency
        converter = SchemaSemanticTableConverter(schema)
        return converter.arrow_table_to_python_dicts(table)

    def python_dicts_to_arrow_table(
        self, dicts: list[dict[str, Any]], schema: SemanticSchema | None = None
    ) -> pa.Table:
        """Convert list of Python dictionaries to Arrow table."""
        if not dicts:
            raise ValueError("Cannot create table from empty list")

        if schema is None:
            # Infer schema from first dictionary
            schema_dict = {key: type(value) for key, value in dicts[0].items()}
            schema = SemanticSchema(schema_dict, self.registry)

        # Use schema-specific converter for efficiency
        converter = SchemaSemanticTableConverter(schema)
        return converter.python_dicts_to_arrow_table(dicts)


# Utility functions for working with semantic tables
def create_semantic_table(
    data: dict[str, Any] | list[dict[str, Any]],
    registry: SemanticTypeRegistry,
) -> pa.Table:
    """Convenience function to create Arrow table with semantic types."""
    converter = SemanticTableConverter(registry)

    if isinstance(data, dict):
        return converter.python_dict_to_arrow_table(data)
    else:
        return converter.python_dicts_to_arrow_table(data)


def extract_python_data(
    table: pa.Table, registry: SemanticTypeRegistry
) -> list[dict[str, Any]]:
    """Convenience function to extract Python data from semantic table."""
    converter = SemanticTableConverter(registry)
    return converter.arrow_table_to_python_dicts(table)
