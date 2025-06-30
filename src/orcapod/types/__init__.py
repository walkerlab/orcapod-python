from .core import Tag, PathLike, PathSet, PodFunction, TypeSpec
from .packets import Packet
from .semantic_type_registry import SemanticTypeRegistry
from .semantic_type_handlers import PathHandler, UUIDHandler, DateTimeHandler
from . import semantic_type_handlers
from . import typespec_utils


# Create default registry and register handlers
default_registry = SemanticTypeRegistry()

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
    "PacketLike"
    "TypeSpec",
    "PathLike",
    "PathSet",
    "PodFunction",
    "semantic_type_handlers",
    "typespec_utils",
]
