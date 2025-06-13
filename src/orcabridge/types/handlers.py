from typing import Any
import pyarrow as pa
from pathlib import Path
from uuid import UUID
from decimal import Decimal
from datetime import datetime, date, time


class PathHandler:
    """Handler for pathlib.Path objects, stored as strings."""

    def supported_types(self) -> type:
        return Path

    def to_storage_type(self) -> pa.DataType:
        return pa.string()

    def to_storage_value(self, value: Path) -> str:
        return str(value)

    def from_storage_value(self, value: str) -> Path | None:
        return Path(value) if value else None


class UUIDHandler:
    """Handler for UUID objects, stored as strings."""

    def supported_types(self) -> type:
        return UUID

    def to_storage_type(self) -> pa.DataType:
        return pa.string()

    def to_storage_value(self, value: UUID) -> str:
        return str(value)

    def from_storage_value(self, value: str) -> UUID | None:
        return UUID(value) if value else None


class DecimalHandler:
    """Handler for Decimal objects, stored as strings."""

    def supported_types(self) -> type:
        return Decimal

    def to_storage_type(self) -> pa.DataType:
        return pa.string()

    def to_storage_value(self, value: Decimal) -> str:
        return str(value)

    def from_storage_value(self, value: str) -> Decimal | None:
        return Decimal(value) if value else None


class SimpleMappingHandler:
    """Handler for basic types that map directly to Arrow."""

    def __init__(self, python_type: type, arrow_type: pa.DataType):
        self._python_type = python_type
        self._arrow_type = arrow_type

    def supported_types(self) -> type:
        return self._python_type

    def to_storage_type(self) -> pa.DataType:
        return self._arrow_type

    def to_storage_value(self, value: Any) -> Any:
        return value  # Direct mapping

    def from_storage_value(self, value: Any) -> Any:
        return value  # Direct mapping


class DirectArrowHandler:
    """Handler for types that map directly to Arrow without conversion."""

    def __init__(self, arrow_type: pa.DataType):
        self._arrow_type = arrow_type

    def supported_types(self) -> type:
        return self._arrow_type

    def to_storage_type(self) -> pa.DataType:
        return self._arrow_type

    def to_storage_value(self, value: Any) -> Any:
        return value  # Direct mapping

    def from_storage_value(self, value: Any) -> Any:
        return value  # Direct mapping


class DateTimeHandler:
    """Handler for datetime objects."""

    def supported_types(self) -> tuple[type, ...]:
        return (datetime, date, time)  # Handles multiple related types

    def to_storage_type(self) -> pa.DataType:
        return pa.timestamp("us")  # Store everything as timestamp

    def to_storage_value(self, value: datetime | date | time) -> Any:
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime.combine(value, time.min)
        elif isinstance(value, time):
            return datetime.combine(date.today(), value)

    def from_storage_value(self, value: datetime) -> datetime:
        return value  # Could add logic to restore original type if needed
