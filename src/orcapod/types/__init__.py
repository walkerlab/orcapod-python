# src/orcabridge/types.py
from .core import Tag, Packet, TypeSpec, PathLike, PathSet, PodFunction
from .registry import TypeRegistry
from .handlers import PathHandler, UUIDHandler, DateTimeHandler
from . import handlers
from . import typespec


# Create default registry and register handlers
default_registry = TypeRegistry()

# Register with semantic names - registry extracts supported types automatically
default_registry.register("path", PathHandler())
default_registry.register("uuid", UUIDHandler())
default_registry.register(
    "datetime", DateTimeHandler()
)  # Registers for datetime, date, time

__all__ = [
    "default_registry",
    "Tag",
    "Packet",
    "TypeSpec",
    "PathLike",
    "PathSet",
    "PodFunction",
    "handlers",
    "typespec",
]
