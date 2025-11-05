"""
Tests for dynamic release fetching functionality.
"""

from unittest.mock import MagicMock, patch

import pytest
from overturemaps.core import get_available_releases, get_latest_release


class TestDynamicReleases:
    """Tests for dynamic release fetching from STAC catalog."""

    @pytest.fixture
    def mock_catalog(self):
        """Mock STAC catalog data."""
        return {
            "latest": "2025-10-22.0",
            "links": [
                {"rel": "child", "href": "./2025-09-24.0/catalog.json"},
                {"rel": "child", "href": "./2025-10-22.0/catalog.json"},
                {"rel": "child", "href": "./2025-08-20.1/catalog.json"},
                {"rel": "self", "href": "./catalog.json"},
            ],
        }

    def test_get_available_releases_success(self, mock_catalog):
        """Test successful fetch of releases from STAC catalog."""
        with patch("overturemaps.core.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            mock_urlopen.return_value = mock_response

            with patch("json.load", return_value=mock_catalog):
                # Clear cache
                import overturemaps.core

                overturemaps.core._cached_releases = None
                overturemaps.core._cached_latest_release = None

                releases, latest = get_available_releases()

        assert "2025-09-24.0" in releases
        assert "2025-10-22.0" in releases
        assert "2025-08-20.1" in releases
        assert latest == "2025-10-22.0"

    def test_get_available_releases_caching(self, mock_catalog):
        """Test that releases are cached after first fetch."""
        with patch("overturemaps.core.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            mock_urlopen.return_value = mock_response

            with patch("json.load", return_value=mock_catalog):
                # Clear cache first
                import overturemaps.core

                overturemaps.core._cached_releases = None
                overturemaps.core._cached_latest_release = None

                # First call
                releases1, latest1 = get_available_releases()

                # Second call should use cache
                releases2, latest2 = get_available_releases()

        # Should only have called urlopen once
        assert mock_urlopen.call_count == 1

        # Results should be identical
        assert releases1 == releases2
        assert latest1 == latest2

    def test_get_available_releases_fallback_on_error(self):
        """Test fallback to hardcoded releases on network error."""
        with patch("overturemaps.core.urlopen", side_effect=Exception("Network error")):
            # Clear cache
            import overturemaps.core

            overturemaps.core._cached_releases = None
            overturemaps.core._cached_latest_release = None

            releases, latest = get_available_releases()

        # Should return fallback releases
        assert isinstance(releases, list)
        assert len(releases) > 0
        assert isinstance(latest, str)

    def test_get_latest_release(self, mock_catalog):
        """Test getting just the latest release."""
        with patch("overturemaps.core.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            mock_urlopen.return_value = mock_response

            with patch("json.load", return_value=mock_catalog):
                # Clear cache
                import overturemaps.core

                overturemaps.core._cached_releases = None
                overturemaps.core._cached_latest_release = None

                latest = get_latest_release()

        assert latest == "2025-10-22.0"

    def test_all_releases_proxy(self, mock_catalog):
        """Test that ALL_RELEASES proxy works correctly."""
        with patch("overturemaps.core.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.__enter__ = lambda self: mock_response
            mock_response.__exit__ = lambda self, *args: None
            mock_urlopen.return_value = mock_response

            with patch("json.load", return_value=mock_catalog):
                # Clear cache
                import overturemaps.core

                overturemaps.core._cached_releases = None
                overturemaps.core._cached_latest_release = None

                from overturemaps.core import ALL_RELEASES

                # Test indexing
                first = ALL_RELEASES[0]
                assert isinstance(first, str)

                # Test iteration
                releases_list = list(ALL_RELEASES)
                assert len(releases_list) > 0

                # Test length
                assert len(ALL_RELEASES) > 0


class TestReleasesIntegration:
    """Integration tests for release fetching."""

    @pytest.mark.integration
    def test_fetch_real_releases(self):
        """Test fetching actual releases from STAC catalog."""
        # Clear cache
        import overturemaps.core

        overturemaps.core._cached_releases = None
        overturemaps.core._cached_latest_release = None

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

        overturemaps.core._cached_releases = None
        overturemaps.core._cached_latest_release = None

        latest = get_latest_release()

        assert isinstance(latest, str)
        # Should match pattern YYYY-MM-DD.N
        parts = latest.split(".")
        assert len(parts) == 2
        date_parts = parts[0].split("-")
        assert len(date_parts) == 3
        assert all(part.isdigit() for part in date_parts)
        assert parts[1].isdigit()
