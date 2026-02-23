"""Tests for the releases module."""

import pytest
from overturemaps import releases


def test_list_releases():
    """Test that list_releases returns a non-empty list."""
    all_releases = releases.list_releases()
    assert isinstance(all_releases, list)
    assert len(all_releases) > 0
    # Should be sorted newest first
    assert all_releases == sorted(all_releases, reverse=True)


def test_get_latest_release():
    """Test that get_latest_release returns a valid release string."""
    latest = releases.get_latest_release()
    assert isinstance(latest, str)
    assert len(latest) > 0
    # Should be in the format YYYY-MM-DD.N
    assert "-" in latest
    assert "." in latest


def test_release_exists():
    """Test release_exists with a known release."""
    latest = releases.get_latest_release()
    assert releases.release_exists(latest) is True
    assert releases.release_exists("invalid-release") is False


def test_get_next_release():
    """Test get_next_release logic."""
    all_releases = releases.list_releases()
    if len(all_releases) < 2:
        pytest.skip("Need at least 2 releases to test get_next_release")
    
    # Latest release should have no next release
    latest = all_releases[0]
    assert releases.get_next_release(latest) is None
    
    # Second-to-last release should return latest
    second_latest = all_releases[1]
    assert releases.get_next_release(second_latest) == latest
    
    # Invalid release should return None
    assert releases.get_next_release("invalid-release") is None
