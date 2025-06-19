"""
Utility functions for handling tags
"""

from collections.abc import Collection, Mapping
from typing import TypeVar, Hashable, Any

from orcapod.types import Packet, Tag, TypeSpec


K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


def get_typespec(dict: Mapping) -> TypeSpec:
    """
    Returns a TypeSpec for the given dictionary.
    The TypeSpec is a mapping from field name to Python type.
    """
    return {key: type(value) for key, value in dict.items()}


def get_compatible_type(type1: Any, type2: Any) -> Any:
    if type1 is type2:
        return type1
    if issubclass(type1, type2):
        return type2
    if issubclass(type2, type1):
        return type1
    raise TypeError(f"Types {type1} and {type2} are not compatible")


def merge_dicts(left: dict[K, V], right: dict[K, V]) -> dict[K, V]:
    merged = left.copy()
    for key, right_value in right.items():
        if key in merged:
            if merged[key] != right_value:
                raise ValueError(
                    f"Conflicting values for key '{key}': {merged[key]} vs {right_value}"
                )
        else:
            merged[key] = right_value
    return merged


def merge_typespecs(left: TypeSpec | None, right: TypeSpec | None) -> TypeSpec | None:
    if left is None:
        return right
    if right is None:
        return left
    # Merge the two TypeSpecs but raise an error if conflicts in types are found
    merged = dict(left)
    for key, right_type in right.items():
        merged[key] = (
            get_compatible_type(merged[key], right_type)
            if key in merged
            else right_type
        )
    return merged


def common_elements(*values) -> Collection[str]:
    """
    Returns the common keys between all lists of values. The identified common elements are
    order preserved with respect to the first list of values
    """
    if len(values) == 0:
        return []
    common_keys = set(values[0])
    for tag in values[1:]:
        common_keys.intersection_update(tag)
    # Preserve the order of the first list of values
    common_keys = [k for k in values[0] if k in common_keys]
    return common_keys


def join_tags(tag1: Mapping[K, V], tag2: Mapping[K, V]) -> dict[K, V] | None:
    """
    Joins two tags together. If the tags have the same key, the value must be the same or None will be returned.
    """
    # create a dict copy of tag1
    joined_tag = dict(tag1)
    for k, v in tag2.items():
        if k in joined_tag and joined_tag[k] != v:
            # Detected a mismatch in the tags, return None
            return None
        else:
            joined_tag[k] = v
    return joined_tag


def check_packet_compatibility(packet1: Packet, packet2: Packet) -> bool:
    """
    Checks if two packets are compatible. If the packets have the same key, the value must be the same or False will be returned.
    If the packets have different keys, they are compatible.
    """
    for k in packet1.keys():
        if k in packet2 and packet1[k] != packet2[k]:
            return False
    return True


def batch_tags(all_tags: Collection[Tag]) -> Tag:
    """
    Batches the tags together. Grouping values under the same key into a list.
    """
    all_keys: set[str] = set()
    for tag in all_tags:
        all_keys.update(tag.keys())
    batch_tag = {key: [] for key in all_keys}  # Initialize batch_tag with all keys
    for tag in all_tags:
        for k in all_keys:
            batch_tag[k].append(
                tag.get(k, None)
            )  # Append the value or None if the key is not present
    return batch_tag


def batch_packet(
    all_packets: Collection[Packet], drop_missing_keys: bool = True
) -> Packet:
    """
    Batches the packets together. Grouping values under the same key into a list.
    If all packets do not have the same key, raise an error unless drop_missing_keys is True
    """
    all_keys: set[str] = set()
    for p in all_packets:
        all_keys.update(p.keys())
    batch_packet = {key: [] for key in all_keys}
    for p in all_packets:
        for k in all_keys:
            if k not in p:
                if drop_missing_keys:
                    continue
                else:
                    raise KeyError(f"Packet {p} does not have key {k}")
            batch_packet[k].append(p[k])
    return batch_packet


def fill_missing(dict, keys, default=None):
    """
    Fill the missing keys in the dictionary with the specified default value.
    """
    for key in keys:
        if key not in dict:
            dict[key] = default
    return dict
