"""Functions for listing and querying Overture Maps S3 releases."""

from __future__ import annotations

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from .core import get_available_releases as _get_available_releases_from_stac

# Overture Maps S3 bucket and prefix layout
OVERTURE_BUCKET = "overturemaps-us-west-2"
RELEASE_PREFIX = "release/"


def _get_s3_client():
    """Return a boto3 S3 client configured for anonymous (public) access."""
    return boto3.client(
        "s3",
        region_name="us-west-2",
        config=Config(signature_version=UNSIGNED),
    )


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
    """Check whether a given release ID exists in the S3 bucket.

    First checks the STAC catalog for efficiency, then falls back to S3 if needed.

    Args:
        release: Release ID to check (e.g. "2024-11-13.0").

    Returns:
        True if the release exists, False otherwise.
    """
    # First check the STAC catalog (faster)
    try:
        releases, _ = _get_available_releases_from_stac()
        if release in releases:
            return True
    except Exception:
        pass  # Fall through to S3 check

    # Fall back to S3 listing for releases not in the catalog
    client = _get_s3_client()
    prefix = f"{RELEASE_PREFIX}{release}/"
    response = client.list_objects_v2(
        Bucket=OVERTURE_BUCKET,
        Prefix=prefix,
        MaxKeys=1,
    )
    return response.get("KeyCount", 0) > 0


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
