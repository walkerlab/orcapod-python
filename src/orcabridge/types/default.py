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
default_registry.register("int", SimpleMappingHandler(int, pa.int64()))
default_registry.register("float", SimpleMappingHandler(float, pa.float64()))
default_registry.register("bool", SimpleMappingHandler(bool, pa.bool_()))
default_registry.register("str", SimpleMappingHandler(str, pa.string()))
default_registry.register("bytes", SimpleMappingHandler(bytes, pa.binary()))
default_registry.register(
    "datetime", DateTimeHandler()
)  # Registers for datetime, date, time
