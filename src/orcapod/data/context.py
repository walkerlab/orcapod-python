from typing import Self
from orcapod.types.semantic_types import SemanticTypeRegistry
from orcapod.types import default_registry
from orcapod.protocols import hashing_protocols as hp
from orcapod.hashing.defaults import get_default_arrow_hasher, get_default_object_hasher
from dataclasses import dataclass


@dataclass
class DataContext:
    context_key: str
    semantic_type_registry: SemanticTypeRegistry
    arrow_hasher: hp.ArrowHasher
    object_hasher: hp.ObjectHasher

    @staticmethod
    def resolve_data_context(data_context: "str | DataContext | None") -> "DataContext":
        """
        Returns the default data context manager.
        This is typically used when no specific context is provided.
        """
        return orcapod_system_data_context_manager.resolve_context(data_context)


default_data_context = DataContext(
    "std:v0.1.0:default",
    default_registry,
    get_default_arrow_hasher(),
    get_default_object_hasher(),
)


class DataContextManager(dict[str, DataContext]):
    def register_context(self, DataContext):
        """
        Register a new DataContext instance.

        Args:
            DataContext: The DataContext instance to register.
        """
        if DataContext.context_key in self:
            raise ValueError(
                f"DataContext with key {DataContext.context_key} already exists."
            )
        self[DataContext.context_key] = DataContext

    def resolve_context(self, context_info: str | DataContext | None) -> DataContext:
        if isinstance(context_info, DataContext):
            return context_info
        if context_info is None:
            return default_data_context
        if isinstance(context_info, str):
            if context_info in self:
                return self[context_info]
            else:
                raise ValueError(f"DataContext with key {context_info} not found.")


orcapod_system_data_context_manager = DataContextManager()
orcapod_system_data_context_manager.register_context(default_data_context)
