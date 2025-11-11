"""
Tests for dynamic release fetching functionality.
"""

import pytest
from overturemaps.core import get_available_releases, get_latest_release


class TestReleasesIntegration:
    """Integration tests for release fetching."""

    @pytest.mark.integration
    def test_fetch_real_releases(self):
        """Test fetching actual releases from STAC catalog."""
        # Clear cache
        import overturemaps.core

        overturemaps.core._cached_stac_catalog = None

        releases, latest = get_available_releases()

        assert isinstance(releases, list)
        assert len(releases) > 0
        assert isinstance(latest, str)
        assert latest in releases

    @pytest.mark.integration
    def test_latest_release_is_valid(self):
        """Test that latest release is a valid version string."""
        # Clear cache
        import overturemaps.core

        overturemaps.core._cached_stac_catalog = None

        latest = get_latest_release()

        assert isinstance(latest, str)
        # Should match pattern YYYY-MM-DD.N
        parts = latest.split(".")
        assert len(parts) == 2
        date_parts = parts[0].split("-")
        assert len(date_parts) == 3
        assert all(part.isdigit() for part in date_parts)
        assert parts[1].isdigit()
