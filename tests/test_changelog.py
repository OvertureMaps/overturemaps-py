"""Tests for the changelog module."""

import pytest
from overturemaps.changelog import query_changelog_ids, summarize_changelog
from overturemaps.models import BBox
from overturemaps.releases import get_latest_release


@pytest.mark.integration
def test_query_changelog_ids():
    """Test querying changelog IDs for a small bbox."""
    latest = get_latest_release()
    
    # Use a small bbox in Austin, TX
    bbox = BBox(xmin=-97.75, ymin=30.25, xmax=-97.74, ymax=30.26)
    
    ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
        latest, "buildings", "building", bbox
    )
    
    # All should be sets
    assert isinstance(ids_to_add, set)
    assert isinstance(ids_to_modify, set)
    assert isinstance(ids_to_delete, set)
    
    # All IDs should be strings
    for id_ in ids_to_add:
        assert isinstance(id_, str)
    for id_ in ids_to_modify:
        assert isinstance(id_, str)
    for id_ in ids_to_delete:
        assert isinstance(id_, str)


@pytest.mark.integration
def test_query_changelog_ids_empty_bbox():
    """Test querying changelog with a bbox that has no changes."""
    latest = get_latest_release()
    
    # Use a small bbox in the middle of the ocean (should have no changes)
    bbox = BBox(xmin=0.0, ymin=0.0, xmax=0.001, ymax=0.001)
    
    ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
        latest, "buildings", "building", bbox
    )
    
    # Should all be empty sets
    assert len(ids_to_add) == 0
    assert len(ids_to_modify) == 0
    assert len(ids_to_delete) == 0


@pytest.mark.integration
def test_summarize_changelog():
    """Test summarizing changelog for a theme."""
    latest = get_latest_release()
    
    # Summarize for buildings theme
    results = summarize_changelog(latest, theme="buildings")
    
    assert isinstance(results, dict)
    assert "buildings" in results
    
    # Should have building and building_part types
    types_data = results["buildings"]
    assert isinstance(types_data, dict)
    
    # Each type should have change counts
    for type_name, change_counts in types_data.items():
        assert isinstance(change_counts, dict)
        # Change counts should be integers
        for change_type, count in change_counts.items():
            assert isinstance(change_type, str)
            assert isinstance(count, int)
            assert count >= 0


@pytest.mark.integration
def test_summarize_changelog_single_type():
    """Test summarizing changelog for a single type."""
    latest = get_latest_release()
    
    # Summarize for building type only
    results = summarize_changelog(latest, theme="buildings", type_="building")
    
    assert isinstance(results, dict)
    assert "buildings" in results
    assert "building" in results["buildings"]
    
    change_counts = results["buildings"]["building"]
    assert isinstance(change_counts, dict)
    
    # Should have some change types
    assert len(change_counts) > 0
