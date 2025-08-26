from abc import ABC
from collections.abc import Collection
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID
from orcapod.protocols import hashing_protocols as hp
from orcapod import contexts
import logging


logger = logging.getLogger(__name__)


class LablableBase:
    def __init__(self, label: str | None = None, **kwargs):
        self._label = label
        super().__init__(**kwargs)

    @property
    def has_assigned_label(self) -> bool:
        """
        Check if the label is explicitly set for this object.

        Returns:
            bool: True if the label is explicitly set, False otherwise.
        """
        return self._label is not None

    @property
    def label(self) -> str:
        """
        Get the label of this object.

        Returns:
            str | None: The label of the object, or None if not set.
        """
        return self._label or self.computed_label() or self.__class__.__name__

    @label.setter
    def label(self, label: str | None) -> None:
        """
        Set the label of this object.

        Args:
            label (str | None): The label to set for this object.
        """
        self._label = label

    def computed_label(self) -> str | None:
        """
        Compute a label for this object based on its content. If label is not explicitly set for this object
        and computed_label returns a valid value, it will be used as label of this object.
        """
        return None


class ContextAwareBase(ABC):
    def __init__(
        self, data_context: str | contexts.DataContext | None = None, **kwargs
    ):
        super().__init__(**kwargs)
        self._data_context = contexts.resolve_context(data_context)

    @property
    def data_context(self) -> contexts.DataContext:
        return self._data_context

    @property
    def data_context_key(self) -> str:
        """Return the data context key."""
        return self._data_context.context_key


class ContentIdentifiableBase(ContextAwareBase):
    """
    Base class for content-identifiable objects.
    This class provides a way to define objects that can be uniquely identified
    based on their content rather than their identity in memory. Specifically, the identity of the
    object is determined by the structure returned by the `identity_structure` method.
    The hash of the object is computed based on the `identity_structure` using the provided `ObjectHasher`,
    which defaults to the one returned by `get_default_object_hasher`.
    Two content-identifiable objects are considered equal if their `identity_structure` returns the same value.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize the ContentHashable with an optional ObjectHasher.

        Args:
            identity_structure_hasher (ObjectHasher | None): An instance of ObjectHasher to use for hashing.
        """
        super().__init__(**kwargs)
        self._cached_content_hash: hp.ContentHash | None = None
        self._cached_int_hash: int | None = None

    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.

        Override this method in your subclass to provide a stable representation
        of your object's content. The structure should contain all fields that
        determine the object's identity.

        Returns:
            Any: A structure representing this object's content, or None to use default hash
        """
        raise NotImplementedError("Subclasses must implement identity_structure")

    def content_hash(self) -> hp.ContentHash:
        """
        Compute a hash based on the content of this object.

        Returns:
            bytes: A byte representation of the hash based on the content.
                   If no identity structure is provided, return None.
        """
        if self._cached_content_hash is None:
            structure = self.identity_structure()
            processed_structure = process_structure(structure)
            self._cached_content_hash = self._data_context.object_hasher.hash_object(
                processed_structure
            )
        return self._cached_content_hash

    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the superclass's hash method.

        Returns:
            int: A hash value based on either content or identity
        """
        # Get the identity structure
        if self._cached_int_hash is None:
            structure = self.identity_structure()
            if structure is None:
                # If no identity structure is provided, use the default hash
                self._cached_int_hash = super().__hash__()
            else:
                self._cached_int_hash = self._data_context.object_hasher.hash_object(
                    structure
                ).to_int()
        return self._cached_int_hash

    def __eq__(self, other: object) -> bool:
        """
        Equality check that compares the identity structures of two objects.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if both objects have the same identity structure, False otherwise.
        """
        if not isinstance(other, ContentIdentifiableBase):
            return NotImplemented

        return self.identity_structure() == other.identity_structure()


class LabeledContentIdentifiableBase(ContentIdentifiableBase, LablableBase):
    pass


def process_structure(
    obj: Any,
    visited: set[int] | None = None,
    force_hash: bool = True,
    function_info_extractor: hp.FunctionInfoExtractor | None = None,
) -> Any:
    """
    Recursively process a structure to prepare it for hashing.

    Args:
        obj: The object or structure to process
        visited: Set of object ids already visited (to handle circular references)
        function_info_extractor: FunctionInfoExtractor to be used for extracting necessary function representation

    Returns:
        A processed version of the structure suitable for stable hashing
    """
    # Initialize the visited set if this is the top-level call
    if visited is None:
        visited = set()
    else:
        visited = visited.copy()  # Copy to avoid modifying the original set

    # Check for circular references - use object's memory address
    # NOTE: While id() is not stable across sessions, we only use it within a session
    # to detect circular references, not as part of the final hash
    obj_id = id(obj)
    if obj_id in visited:
        logger.debug(
            f"Detected circular reference for object of type {type(obj).__name__}"
        )
        return "CircularRef"  # Don't include the actual id in hash output

    # For objects that could contain circular references, add to visited
    if isinstance(obj, (dict, list, tuple, set)) or not isinstance(
        obj, (str, int, float, bool, type(None))
    ):
        visited.add(obj_id)

    # Handle None
    if obj is None:
        return None

    # TODO: currently using runtime_checkable on ContentIdentifiable protocol
    # Re-evaluate this strategy to see if a faster / more robust check could be used
    if isinstance(obj, hp.ContentIdentifiable):
        logger.debug(
            f"Processing ContentHashableBase instance of type {type(obj).__name__}"
        )
        return obj.content_hash()

    # Handle basic types
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Handle bytes and bytearray
    if isinstance(obj, (bytes, bytearray)):
        logger.debug(
            f"Converting bytes/bytearray of length {len(obj)} to hex representation"
        )
        return obj.hex()

    # Handle Path objects
    if isinstance(obj, Path):
        logger.debug(f"Converting Path object to string: {obj}")
        raise NotImplementedError(
            "Path objects are not supported in this hasher. Please convert to string."
        )
        return str(obj)

    # Handle UUID objects
    if isinstance(obj, UUID):
        logger.debug(f"Converting UUID to string: {obj}")
        raise NotImplementedError(
            "UUID objects are not supported in this hasher. Please convert to string."
        )
        return str(obj)

    # Handle named tuples (which are subclasses of tuple)
    if hasattr(obj, "_fields") and isinstance(obj, tuple):
        logger.debug(f"Processing named tuple of type {type(obj).__name__}")
        # For namedtuples, convert to dict and then process
        d = {field: getattr(obj, field) for field in obj._fields}  # type: ignore
        return process_structure(d, visited)

    # Handle mappings (dict-like objects)
    if isinstance(obj, Mapping):
        # Process both keys and values
        processed_items = [
            (
                process_structure(k, visited),
                process_structure(v, visited),
            )
            for k, v in obj.items()
        ]

        # Sort by the processed keys for deterministic order
        processed_items.sort(key=lambda x: str(x[0]))

        # Create a new dictionary with string keys based on processed keys
        # TODO: consider checking for possibly problematic values in processed_k
        # and issue a warning
        return {
            str(processed_k): processed_v
            for processed_k, processed_v in processed_items
        }

    # Handle sets and frozensets
    if isinstance(obj, (set, frozenset)):
        logger.debug(
            f"Processing set/frozenset of type {type(obj).__name__} with {len(obj)} items"
        )
        # Process each item first, then sort the processed results
        processed_items = [process_structure(item, visited) for item in obj]
        return sorted(processed_items, key=str)

    # Handle collections (list-like objects)
    if isinstance(obj, Collection):
        logger.debug(
            f"Processing collection of type {type(obj).__name__} with {len(obj)} items"
        )
        return [process_structure(item, visited) for item in obj]

    # For functions, use the function_content_hash
    if callable(obj) and hasattr(obj, "__code__"):
        logger.debug(f"Processing function: {getattr(obj, '__name__')}")
        if function_info_extractor is not None:
            # Use the extractor to get a stable representation
            function_info = function_info_extractor.extract_function_info(obj)
            logger.debug(f"Extracted function info: {function_info} for {obj.__name__}")

            # simply return the function info as a stable representation
            return function_info
        else:
            raise ValueError(
                f"Function {obj} encountered during processing but FunctionInfoExtractor is missing"
            )

    # handle data types
    if isinstance(obj, type):
        logger.debug(f"Processing class/type: {obj.__name__}")
        return f"type:{obj.__name__}"

    # For other objects, attempt to create deterministic representation only if force_hash=True
    class_name = obj.__class__.__name__
    module_name = obj.__class__.__module__
    if force_hash:
        try:
            import re

            logger.debug(
                f"Processing generic object of type {module_name}.{class_name}"
            )

            # Try to get a stable dict representation if possible
            if hasattr(obj, "__dict__"):
                # Sort attributes to ensure stable order
                attrs = sorted(
                    (k, v) for k, v in obj.__dict__.items() if not k.startswith("_")
                )
                # Limit to first 10 attributes to avoid extremely long representations
                if len(attrs) > 10:
                    logger.debug(
                        f"Object has {len(attrs)} attributes, limiting to first 10"
                    )
                    attrs = attrs[:10]
                attr_strs = [f"{k}={type(v).__name__}" for k, v in attrs]
                obj_repr = f"{{{', '.join(attr_strs)}}}"
            else:
                # Get basic repr but remove memory addresses
                logger.debug(
                    "Object has no __dict__, using repr() with memory address removal"
                )
                obj_repr = repr(obj)
                if len(obj_repr) > 1000:
                    logger.debug(
                        f"Object repr is {len(obj_repr)} chars, truncating to 1000"
                    )
                    obj_repr = obj_repr[:1000] + "..."
                # Remove memory addresses which look like '0x7f9a1c2b3d4e'
                obj_repr = re.sub(r" at 0x[0-9a-f]+", " at 0xMEMADDR", obj_repr)

            return f"{module_name}.{class_name}:{obj_repr}"
        except Exception as e:
            # Last resort - use class name only
            logger.warning(f"Failed to process object representation: {e}")
            try:
                return f"object:{obj.__class__.__module__}.{obj.__class__.__name__}"
            except AttributeError:
                logger.error("Could not determine object class, using UnknownObject")
                return "UnknownObject"
    else:
        raise ValueError(
            f"Processing of {obj} of type {module_name}.{class_name} is not supported"
        )
