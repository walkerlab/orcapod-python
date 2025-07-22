from orcapod.types.semantic_types import PythonArrowConverter
from orcapod.types.schemas import PythonSchema, SemanticSchema
from orcapod.types import TypeSpec, typespec_utils as tsutils

from typing import Any, Self
from collections.abc import Mapping
import pyarrow as pa
import logging

logger = logging.getLogger(__name__)


class SemanticConverter:
    @classmethod
    def from_semantic_schema(cls, semantic_schema: SemanticSchema) -> Self:
        converter_lut = {}
        for (
            field,
            semantic_type,
        ) in semantic_schema.get_semantic_fields().items():
            converter_lut[field] = PythonArrowConverter.from_semantic_type(
                semantic_type
            )
        return cls(converter_lut)

    def __init__(
        self,
        converter_lut: dict[str, PythonArrowConverter],
    ):
        self._converter_lut = converter_lut

    def from_python_to_arrow_schema(self, python_schema: TypeSpec) -> pa.Schema:
        """Convert a Python schema to an Arrow schema"""
        return PythonSchema(python_schema).to_arrow_schema(
            converters=self._converter_lut
        )

    def from_arrow_to_python_schema(self, arrow_schema: pa.Schema) -> PythonSchema:
        """Convert an Arrow schema to a Python schema"""
        return PythonSchema.from_arrow_schema(
            arrow_schema, converters=self._converter_lut
        )

    def from_python_to_arrow(
        self, python_data: Mapping[str, Any], python_schema: TypeSpec | None = None
    ) -> pa.Table:
        """Convert a dictionary of Python values to Arrow arrays"""
        if python_schema is None:
            # infer schema from data
            python_schema = PythonSchema(tsutils.get_typespec_from_dict(python_data))
            logger.warning(
                f"Inferred schema {python_schema} from Python data {python_data}. Note that this may not behave as expected."
            )

        arrow_schema = self.from_python_to_arrow_schema(python_schema)

        arrow_data = {}
        for field, value in python_data.items():
            if field in self._converter_lut:
                converter = self._converter_lut[field]
                arrow_data[field] = converter.from_python_to_arrow(value)
            else:
                arrow_data[field] = [value]
        return pa.Table.from_pydict(arrow_data, schema=arrow_schema)

    def from_arrow_to_python(self, arrow_data: pa.Table) -> list[dict[str, Any]]:
        """Convert a dictionary of Arrow arrays to Python values"""

        values = []
        for column_name in arrow_data.column_names:
            column = arrow_data[column_name]
            if column_name not in self._converter_lut:
                values.append(column.to_pylist())
            else:
                converter = self._converter_lut[column_name]
                values.append(converter.from_arrow_to_python(column))
        all_entries = []

        for entry in zip(*values):
            assert len(entry) == len(arrow_data.column_names), (
                "Mismatch in number of columns and values"
            )
            all_entries.append(dict(zip(arrow_data.column_names, entry)))

        return all_entries

    def as_dict(self) -> dict[str, PythonArrowConverter]:
        """Return the converter lookup table as a dictionary."""
        return self._converter_lut.copy()

    def join(self, other: Self, strict: bool = False) -> Self:
        """Join two SemanticConverters by merging their converter lookup tables."""
        if not isinstance(other, SemanticConverter):
            raise TypeError("Can only join with another SemanticConverter.")

        new_converter_lut = self._converter_lut.copy()
        for key, converter in other._converter_lut.items():
            if key in new_converter_lut:
                if strict:
                    raise ValueError(
                        f"Key '{key}' already exists in the converter lookup table. Cannot overwrite in strict mode."
                    )
                logger.warning(
                    f"Key '{key}' already exists in the converter lookup table. Overwriting with new converter."
                )
            new_converter_lut[key] = converter

        return self.__class__(new_converter_lut)

    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        """Rename columns in the converter lookup table."""
        new_converter_lut = {}
        new_converter_lut = {
            column_mapping.get(key, key): converter
            for key, converter in self._converter_lut.items()
        }

        return self.__class__(new_converter_lut)
