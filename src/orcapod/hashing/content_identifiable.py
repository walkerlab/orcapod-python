from orcapod.hashing.types import ObjectHasher
from orcapod.hashing.defaults import get_default_object_hasher
from typing import Any


class ContentIdentifiableBase:
    """
    Base class for content-identifiable objects.
    This class provides a way to define objects that can be uniquely identified
    based on their content rather than their identity in memory. Specifically, the identity of the
    object is determined by the structure returned by the `identity_structure` method.
    The hash of the object is computed based on the `identity_structure` using the provided `ObjectHasher`,
    which defaults to the one returned by `get_default_object_hasher`.
    Two content-identifiable objects are considered equal if their `identity_structure` returns the same value.
    """

    def __init__(
        self,
        identity_structure_hasher: ObjectHasher | None = None,
        label: str | None = None,
    ) -> None:
        """
        Initialize the ContentHashable with an optional ObjectHasher.

        Args:
            identity_structure_hasher (ObjectHasher | None): An instance of ObjectHasher to use for hashing.
        """
        self.identity_structure_hasher = (
            identity_structure_hasher or get_default_object_hasher()
        )
        self._label = label

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
        if structure is None:
            # If no identity structure is provided, use the default hash
            return super().__hash__()

        return self.identity_structure_hasher.hash_to_int(structure)

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
