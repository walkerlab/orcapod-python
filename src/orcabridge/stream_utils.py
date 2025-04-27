"""
Utility functions for handling tags
"""
from _collections_abc import dict_keys
from typing import List, Dict, Optional, Any, TypeVar, Set, Union, Sequence, Mapping
from .types import Tag, Packet

K = TypeVar('K')
V = TypeVar('V')
def join_tags(tag1: Mapping[K, V], tag2: Mapping[K, V]) -> Optional[Mapping[K, V]]:
    """
    Joins two tags together. If the tags have the same key, the value must be the same or None will be returned.
    """
    joined_tag = dict(tag1)
    for k, v in tag2.items():
        if k in joined_tag and joined_tag[k] != v:
            # Detected a mismatch in the tags, return None
            return None
        else:
            joined_tag[k] = v
    return joined_tag

def batch_tag(all_tags: Sequence[Tag]) -> Tag:
    """
    Batches the tags together. Grouping values under the same key into a list.
    """
    all_keys: Set[str] = set()
    for tag in all_tags:
        all_keys.update(tag.keys())
    batch_tag = {key: [] for key in all_keys}  # Initialize batch_tag with all keys
    for tag in all_tags:
        for k in all_keys:
            batch_tag[k].append(tag.get(k, None))  # Append the value or None if the key is not present
    return batch_tag

def batch_packet(all_packets: Sequence[Packet], drop_missing_keys: bool = True) -> Packet:
    """
    Batches the packets together. Grouping values under the same key into a list.
    If all packets do not have the same key, raise an error unless drop_missing_keys is True
    """
    all_keys: Set[str] = set()
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
