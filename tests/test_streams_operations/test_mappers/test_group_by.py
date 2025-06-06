"""Tests for GroupBy mapper functionality."""

import pytest
import pickle
from orcabridge.mappers import GroupBy
from orcabridge.streams import SyncStreamFromLists


class TestGroupBy:
    """Test cases for GroupBy mapper."""

    def test_group_by_basic(self):
        """Test basic groupby functionality."""
        tags = [
            {"category": "A", "id": "1"},
            {"category": "B", "id": "2"},
            {"category": "A", "id": "3"},
            {"category": "B", "id": "4"},
        ]
        packets = [
            {"value": "data/item1.txt", "name": "metadata/item1.json"},
            {"value": "data/item2.txt", "name": "metadata/item2.json"},
            {"value": "data/item3.txt", "name": "metadata/item3.json"},
            {"value": "data/item4.txt", "name": "metadata/item4.json"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(group_keys=["category"])
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        # Should have 2 groups (A and B)
        assert len(results) == 2

        # Check that all groups are present
        categories_found = []
        for tag, _ in results:
            categories_found.extend(tag["category"])
        categories = set(categories_found)
        assert categories == {"A", "B"}

        # Check grouped data structure
        # With reduce_keys=False (default), everything should be lists including group keys
        for tag, packet in results:
            if tag["category"] == ["A", "A"]:  # Group key is also a list
                assert tag["id"] == ["1", "3"]  # IDs for category A
                assert packet["value"] == [
                    "data/item1.txt",
                    "data/item3.txt",
                ]  # Values for category A
                assert packet["name"] == ["metadata/item1.json", "metadata/item3.json"]
            elif tag["category"] == ["B", "B"]:  # Group key is also a list
                assert tag["id"] == ["2", "4"]  # IDs for category B
                assert packet["value"] == [
                    "data/item2.txt",
                    "data/item4.txt",
                ]  # Values for category B
                assert packet["name"] == ["metadata/item2.json", "metadata/item4.json"]

    def test_group_by_reduce_keys(self):
        """Test groupby with reduce_keys=True."""
        tags = [
            {"category": "A", "id": "1", "extra": "x1"},
            {"category": "A", "id": "2", "extra": "x2"},
            {"category": "B", "id": "3", "extra": "x3"},
        ]
        packets = [
            {"value": "data/item1.txt"},
            {"value": "data/item2.txt"},
            {"value": "data/item3.txt"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(group_keys=["category"], reduce_keys=True)
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        for tag, packet in results:
            if tag["category"] == "A":
                # With reduce_keys=True, group keys become singular values
                assert tag["category"] == "A"
                # Non-group keys become lists
                assert tag["id"] == ["1", "2"]
                assert tag["extra"] == ["x1", "x2"]
            elif tag["category"] == "B":
                assert tag["category"] == "B"
                assert tag["id"] == ["3"]
                assert tag["extra"] == ["x3"]

    def test_group_by_no_group_keys(self):
        """Test groupby without specifying group_keys (uses all tag keys)."""
        tags = [
            {"category": "A", "id": "1"},
            {"category": "A", "id": "1"},  # Duplicate
            {"category": "B", "id": "2"},
        ]
        packets = [
            {"value": "data/item1.txt"},
            {"value": "data/item2.txt"},
            {"value": "data/item3.txt"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy()  # No group_keys specified
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        # Should group by all tag keys (category, id)
        assert len(results) == 2  # (A,1) and (B,2)

        # Extract group keys, accounting for lists in the results
        group_keys = set()
        for tag, _ in results:
            # When reduce_keys=False, all values are lists
            category_list = tag["category"]
            id_list = tag["id"]
            # Since this groups by exact matches, each group should have same values
            # We'll take the first value from each list to represent the group
            group_keys.add((category_list[0], id_list[0]))
        assert group_keys == {("A", "1"), ("B", "2")}

    def test_group_by_with_selection_function(self):
        """Test groupby with selection function."""
        tags = [
            {"category": "A", "priority": "1"},
            {"category": "A", "priority": "2"},
            {"category": "A", "priority": "3"},
        ]
        packets = [
            {"value": "data/item1.txt"},
            {"value": "data/item2.txt"},
            {"value": "data/item3.txt"},
        ]

        # Selection function that only keeps items with priority >= 2
        def select_high_priority(grouped_items):
            return [int(tag["priority"]) >= 2 for tag, packet in grouped_items]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(
            group_keys=["category"], selection_function=select_high_priority
        )
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        assert len(results) == 1
        tag, packet = results[0]

        # Should only have priority 2 and 3 items
        assert tag["priority"] == ["2", "3"]
        assert packet["value"] == ["data/item2.txt", "data/item3.txt"]

    def test_group_by_empty_stream(self):
        """Test groupby with empty stream."""
        stream = SyncStreamFromLists(
            tags=[], packets=[], tag_keys=["category", "id"], packet_keys=["value"]
        )
        group_by = GroupBy(group_keys=["category"])
        grouped_stream = group_by(stream)

        results = list(grouped_stream)
        assert len(results) == 0

    def test_group_by_single_item(self):
        """Test groupby with single item."""
        tags = [{"category": "A", "id": "1"}]
        packets = [{"value": "data/item1.txt"}]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(group_keys=["category"])
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        assert len(results) == 1
        tag, packet = results[0]
        assert tag["category"] == [
            "A"
        ]  # With reduce_keys=False, even single values become lists
        assert tag["id"] == ["1"]
        assert packet["value"] == ["data/item1.txt"]

    def test_group_by_missing_group_keys(self):
        """Test groupby when some items don't have the group keys."""
        tags = [
            {"category": "A", "id": "1"},
            {"id": "2"},  # Missing category
            {"category": "A", "id": "3"},
        ]
        packets = [
            {"value": "data/item1.txt"},
            {"value": "data/item2.txt"},
            {"value": "data/item3.txt"},
        ]

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(group_keys=["category"])
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        # Should have 2 groups: category="A" and category=None
        assert len(results) == 2

        categories = set()
        for tag, _ in results:
            # When reduce_keys=False, all values are lists
            category_list = tag.get("category", [None])
            if category_list and category_list != [None]:
                categories.add(category_list[0])
            else:
                categories.add(None)
        assert categories == {"A", None}

    def test_group_by_selection_function_filters_all(self):
        """Test groupby where selection function filters out all items."""
        tags = [
            {"category": "A", "priority": "1"},
            {"category": "A", "priority": "2"},
        ]
        packets = [
            {"value": "data/item1.txt"},
            {"value": "data/item2.txt"},
        ]

        # Selection function that filters out everything
        def select_none(grouped_items):
            return [False] * len(grouped_items)

        stream = SyncStreamFromLists(tags=tags, packets=packets)
        group_by = GroupBy(group_keys=["category"], selection_function=select_none)
        grouped_stream = group_by(stream)

        results = list(grouped_stream)

        # Should have no results since everything was filtered out
        assert len(results) == 0

    def test_group_by_multiple_streams_error(self):
        """Test that GroupBy raises error with multiple streams."""
        stream1 = SyncStreamFromLists(tags=[{"a": "1"}], packets=[{"b": "file.txt"}])
        stream2 = SyncStreamFromLists(tags=[{"c": "3"}], packets=[{"d": "file2.txt"}])

        group_by = GroupBy(group_keys=["a"])

        with pytest.raises(ValueError, match="exactly one stream"):
            list(group_by(stream1, stream2))

    def test_group_by_pickle(self):
        """Test that GroupBy mapper is pickleable."""
        # Test basic GroupBy
        group_by = GroupBy(group_keys=["category"])
        pickled = pickle.dumps(group_by)
        unpickled = pickle.loads(pickled)

        assert unpickled.group_keys == group_by.group_keys
        assert unpickled.reduce_keys == group_by.reduce_keys
        assert unpickled.selection_function == group_by.selection_function

        # Test with reduce_keys
        group_by_reduce = GroupBy(group_keys=["category"], reduce_keys=True)
        pickled_reduce = pickle.dumps(group_by_reduce)
        unpickled_reduce = pickle.loads(pickled_reduce)

        assert unpickled_reduce.group_keys == group_by_reduce.group_keys
        assert unpickled_reduce.reduce_keys == group_by_reduce.reduce_keys

    def test_group_by_identity_structure(self):
        """Test GroupBy identity_structure method."""
        stream = SyncStreamFromLists(tags=[{"a": "1"}], packets=[{"b": "file.txt"}])

        # Test without selection function
        group_by1 = GroupBy(group_keys=["category"])
        structure1 = group_by1.identity_structure(stream)
        assert structure1[0] == "GroupBy"
        assert structure1[1] == ["category"]
        assert not structure1[2]  # reduce_keys

        # Test with reduce_keys
        group_by2 = GroupBy(group_keys=["category"], reduce_keys=True)
        structure2 = group_by2.identity_structure(stream)
        assert structure2[2]  # reduce_keys

        # Different group_keys should have different structures
        group_by3 = GroupBy(group_keys=["other"])
        structure3 = group_by3.identity_structure(stream)
        assert structure1 != structure3

    def test_group_by_repr(self):
        """Test GroupBy string representation."""
        group_by = GroupBy(group_keys=["category"], reduce_keys=True)
        repr_str = repr(group_by)
        # Should contain class name and key parameters
        assert "GroupBy" in repr_str
