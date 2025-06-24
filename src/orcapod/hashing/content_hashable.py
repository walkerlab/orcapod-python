
from .types import ObjectHasher
from .defaults import get_default_object_hasher
from typing import Any

class ContentHashableBase:
    def __init__(self, object_hasher: ObjectHasher | None = None) -> None:
        """
        Initialize the ContentHashable with an optional ObjectHasher.

        Args:
            object_hasher (ObjectHasher | None): An instance of ObjectHasher to use for hashing.
        """
        self.object_hasher = object_hasher or get_default_object_hasher()


    def identity_structure(self) -> Any:
        """
        Return a structure that represents the identity of this object.

        Override this method in your subclass to provide a stable representation
        of your object's content. The structure should contain all fields that
        determine the object's identity.

        Returns:
            Any: A structure representing this object's content, or None to use default hash
        """
        return None


    def __hash__(self) -> int:
        """
        Hash implementation that uses the identity structure if provided,
        otherwise falls back to the superclass's hash method.

        Returns:
            int: A hash value based on either content or identity
        """
        # Get the identity structure
        structure = self.identity_structure()

        return self.object_hasher.hash_to_int(structure)
    
    def __eq__(self, other: object) -> bool:
        """
        Equality check that compares the identity structures of two objects.

        Args:
            other (object): The object to compare against.

        Returns:
            bool: True if both objects have the same identity structure, False otherwise.
        """
        if not isinstance(other, ContentHashableBase):
            return NotImplemented

        return self.identity_structure() == other.identity_structure()