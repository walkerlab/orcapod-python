"""
Utility functions for handling tags
"""


def join_tags(tag1: dict, tag2: dict) -> dict:
    """
    Joins to tags together. If the tags have the same key, the value must be the same or None will be returned
    """
    joined_tag = dict(tag1)
    for k, v in tag2.items():
        if k in joined_tag and joined_tag[k] != v:
            # Detected a mismatch in the tags, return None
            return None
        else:
            joined_tag[k] = v
    return joined_tag

def batch_tags(tags: list[dict]) -> dict:
    """
    Batches the tags together. Grouping values under the same key into a list.
    """
    all_keys = set()
    for tag in tags:
        all_keys.update(tag.keys())
    batch_tag = {key: [] for key in all_keys}  # Initialize batch_tag with all keys
    for tag in tags:
        for k in all_keys:
            batch_tag[k].append(tag.get(k, None))  # Append the value or None if the key is not present
    return batch_tag