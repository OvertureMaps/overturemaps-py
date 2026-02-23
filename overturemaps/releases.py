"""Functions for listing and querying Overture Maps releases."""

from __future__ import annotations

from .core import get_available_releases as _get_available_releases_from_stac


def list_releases() -> list[str]:
    """List all available Overture Maps release IDs, newest first.

    Uses the STAC catalog for efficient retrieval.

    Returns:
        Sorted list of release IDs (e.g. ["2024-11-13.0", "2024-10-23.0", ...]).
    """
    releases, _ = _get_available_releases_from_stac()
    # Sort descending so newest is first
    return sorted(releases, reverse=True)


def get_latest_release() -> str:
    """Return the ID of the most recent Overture Maps release.

    Uses the STAC catalog for efficient retrieval.

    Returns:
        Latest release ID string.

    Raises:
        RuntimeError: If no releases are found.
    """
    releases, latest = _get_available_releases_from_stac()
    if not latest and not releases:
        raise RuntimeError("No Overture Maps releases found.")
    return latest or releases[0]


def release_exists(release: str) -> bool:
    """Check whether a given release ID exists.

    Uses the STAC catalog.

    Args:
        release: Release ID to check (e.g. "2024-11-13.0").

    Returns:
        True if the release exists, False otherwise.
    """
    try:
        releases, _ = _get_available_releases_from_stac()
        return release in releases
    except Exception:
        return False


def get_next_release(current_release: str) -> str | None:
    """Get the release that comes immediately after the given release.

    Args:
        current_release: A release ID (e.g. "2025-12-17.0").

    Returns:
        The next release ID, or None if current_release is the latest.
    """
    releases = list_releases()  # Returns newest first
    try:
        current_idx = releases.index(current_release)
        # Since list is newest first, next release is at index - 1
        if current_idx > 0:
            return releases[current_idx - 1]
        return None  # Already at latest
    except ValueError:
        # Current release not found
        return None
