from typing import Any
import pyarrow as pa
from pathlib import Path
from uuid import UUID
from decimal import Decimal
from datetime import datetime, date, time


class PathHandler:
    """Handler for pathlib.Path objects, stored as strings."""

    def python_type(self) -> type:
        return Path

    def storage_type(self) -> pa.DataType:
        return pa.string()

    def python_to_storage(self, value: Path) -> str:
        return str(value)

    def storage_to_python(self, value: str) -> Path | None:
        return Path(value) if value else None


class UUIDHandler:
    """Handler for UUID objects, stored as strings."""

    def python_type(self) -> type:
        return UUID

    def storage_type(self) -> pa.DataType:
        return pa.string()

    def python_to_storage(self, value: UUID) -> str:
        return str(value)

    def storage_to_python(self, value: str) -> UUID | None:
        return UUID(value) if value else None


class DecimalHandler:
    """Handler for Decimal objects, stored as strings."""

    def python_type(self) -> type:
        return Decimal

    def storage_type(self) -> pa.DataType:
        return pa.string()

    def python_to_storage(self, value: Decimal) -> str:
        return str(value)

    def storage_to_python(self, value: str) -> Decimal | None:
        return Decimal(value) if value else None


class SimpleMappingHandler:
    """Handler for basic types that map directly to Arrow."""

    def __init__(self, python_type: type, arrow_type: pa.DataType):
        self._python_type = python_type
        self._arrow_type = arrow_type

    def python_type(self) -> type:
        return self._python_type

    def storage_type(self) -> pa.DataType:
        return self._arrow_type

    def python_to_storage(self, value: Any) -> Any:
        return value  # Direct mapping

    def storage_to_python(self, value: Any) -> Any:
        return value  # Direct mapping


class DirectArrowHandler:
    """Handler for types that map directly to Arrow without conversion."""

    def __init__(self, arrow_type: pa.DataType):
        self._arrow_type = arrow_type

    def python_type(self) -> type:
        return self._arrow_type

    def storage_type(self) -> pa.DataType:
        return self._arrow_type

    def python_to_storage(self, value: Any) -> Any:
        return value  # Direct mapping

    def storage_to_python(self, value: Any) -> Any:
        return value  # Direct mapping


class DateTimeHandler:
    """Handler for datetime objects."""

    def python_type(self) -> type:
        return (datetime, date, time)  # Handles multiple related types

    def storage_type(self) -> pa.DataType:
        return pa.timestamp("us")  # Store everything as timestamp

    def python_to_storage(self, value: datetime | date | time) -> Any:
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime.combine(value, time.min)
        elif isinstance(value, time):
            return datetime.combine(date.today(), value)

    def storage_to_python(self, value: datetime) -> datetime:
        return value  # Could add logic to restore original type if needed
