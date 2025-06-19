from .registry import TypeRegistry
from .handlers import (
    PathHandler,
    UUIDHandler,
    SimpleMappingHandler,
    DateTimeHandler,
)
import pyarrow as pa

# Create default registry and register handlers
default_registry = TypeRegistry()

# Register with semantic names - registry extracts supported types automatically
default_registry.register("path", PathHandler())
default_registry.register("uuid", UUIDHandler())
default_registry.register(
    "datetime", DateTimeHandler()
)  # Registers for datetime, date, time
