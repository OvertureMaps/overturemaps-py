"""Functions for querying and processing the Overture Maps GERS changelog.

This module uses pyarrow for reading and filtering changelog data from S3.
It supports STAC catalog acceleration when available.

STAC Support
------------
This module is prepared to use the STAC catalog for accelerated spatial queries
when changelog files are added to the STAC index. The _get_changelog_files_from_stac()
function will automatically enable this optimization once the catalog includes changelog
partitions. Until then, queries fall back to direct S3 path scanning.

See: https://stac.overturemaps.org/
"""

from __future__ import annotations

import io
from typing import Optional
from urllib.request import urlopen

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from .models import BBox

# S3 path template for the changelog Parquet files
CHANGELOG_S3_PATH_TEMPLATE = (
    "overturemaps-us-west-2/changelog/{release}/theme={theme}/type={type}/"
)


def _get_changelog_files_from_stac(
    theme: str, type_: str, bbox: BBox, release: str
) -> Optional[list[str]]:
    """Get changelog file paths from STAC catalog for spatial query optimization.

    This function checks the STAC catalog for changelog file entries and returns
    only the files whose bounding boxes intersect with the query bbox. This can
    dramatically reduce query time by avoiding full S3 partition scans.

    NOTE: As of early 2026, changelog files are not yet included in the STAC catalog.
    This function is prepared for when that feature becomes available. Until then,
    it returns None, causing queries to fall back to direct S3 path scanning.

    Args:
        theme: Overture theme name (e.g., "buildings").
        type_: Overture feature type (e.g., "building").
        bbox: Bounding box for spatial filtering.
        release: Overture release ID (e.g., "2025-01-21.0").

    Returns:
        List of S3 paths (bucket/key format) if STAC has changelog data, None otherwise.
    """
    stac_changelog_url = f"https://stac.overturemaps.org/{release}/changelog.parquet"

    try:
        # Try to read the STAC changelog index
        with urlopen(stac_changelog_url) as response:
            data = response.read()
            buffer = io.BytesIO(data)
            stac_table = pq.read_table(buffer)

        # Filter by theme/type
        feature_type_filter = (pc.field("theme") == theme) & (pc.field("type") == type_)

        # Spatial filter: only include files whose bbox overlaps query bbox
        bbox_filter = (
            (pc.field("bbox", "xmin") <= bbox.xmax)
            & (pc.field("bbox", "xmax") >= bbox.xmin)
            & (pc.field("bbox", "ymin") <= bbox.ymax)
            & (pc.field("bbox", "ymax") >= bbox.ymin)
        )

        combined_filter = feature_type_filter & bbox_filter
        filtered_table = stac_table.filter(combined_filter)

        if filtered_table.num_rows > 0:
            # Extract S3 paths
            file_paths = filtered_table.column("assets").to_pylist()
            s3_paths = [
                path["aws"]["alternate"]["s3"]["href"][len("s3://") :]
                for path in file_paths
            ]
            return s3_paths
        else:
            # No matching files in this region
            return []

    except Exception:
        # STAC changelog not available yet, return None to trigger fallback
        return None


def query_changelog_ids(
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
) -> dict[str, set[str]]:
    """Query changelog and return classified ID sets using pyarrow.

    This function reads changelog Parquet files from S3 and returns sets of IDs
    for added, modified, and removed features within the specified bbox.

    Automatically attempts to use STAC catalog for query acceleration. When changelog
    files are added to STAC, this will speed up spatial queries. Until then, transparently
    falls back to direct S3 access.

    Args:
        release: Overture release ID (e.g. "2024-11-13.0").
        theme: Overture theme name (e.g. "buildings").
        type_: Overture feature type (e.g. "building").
        bbox: Bounding box to spatially filter changes.

    Returns:
        Dictionary mapping change_type to sets of feature IDs.
    """
    # Try STAC first (will automatically work when changelog added to STAC)
    s3_paths = _get_changelog_files_from_stac(theme, type_, bbox, release)

    # Create S3 filesystem
    s3_fs = fs.S3FileSystem(anonymous=True, region="us-west-2")

    # Use STAC paths if available, otherwise use full partition path
    if s3_paths is not None:
        if len(s3_paths) == 0:
            # No files intersect the bbox
            return {}
        # Use specific files from STAC
        dataset_path = s3_paths
    else:
        # STAC not available, use full path pattern
        dataset_path = CHANGELOG_S3_PATH_TEMPLATE.format(
            release=release, theme=theme, type=type_
        )

    try:
        # Create dataset
        dataset = ds.dataset(
            dataset_path,
            filesystem=s3_fs,
            format="parquet",
            partitioning="hive",
        )

        # Build spatial filter
        spatial_filter = (
            (pc.field("bbox", "xmin") <= bbox.xmax)
            & (pc.field("bbox", "xmax") >= bbox.xmin)
            & (pc.field("bbox", "ymin") <= bbox.ymax)
            & (pc.field("bbox", "ymax") >= bbox.ymin)
            & (pc.field("change_type") != "unchanged")
        )

        # Read only id and change_type columns with filter
        table = dataset.to_table(filter=spatial_filter, columns=["id", "change_type"])

        # Group IDs by change_type
        changes: dict[str, set[str]] = {}

        if table.num_rows > 0:
            ids = table.column("id").to_pylist()
            change_types = table.column("change_type").to_pylist()

            for id_, change_type in zip(ids, change_types):
                changes.setdefault(change_type, set()).add(id_)

        return changes

    except FileNotFoundError:
        # If no data found (e.g., missing changelog files), return empty dict
        return {}
    except Exception as e:
        if "No such file" in str(e) or "does not exist" in str(e):
            return {}
        raise


def summarize_changelog(
    release: str,
    theme: str | None = None,
    type_: str | None = None,
) -> dict[str, dict[str, dict[str, int]]]:
    """Return change counts by type for one or more themes without bbox filtering.

    Args:
        release: Overture release ID.
        theme: Overture theme name (optional, defaults to all themes).
        type_: Overture feature type (optional, defaults to all types in theme).

    Returns:
        Nested dictionary: {theme: {type: {change_type: count}}}.
    """
    from .core import type_theme_map

    results = {}
    s3_fs = fs.S3FileSystem(anonymous=True, region="us-west-2")

    # Determine which theme/type combinations to query
    if theme and type_:
        if type_ not in type_theme_map:
            raise ValueError(f"Unknown type: {type_}")
        themes_types = [(theme, type_)]
    elif theme:
        types = _get_types_for_theme(theme)
        themes_types = [(theme, t) for t in types]
    elif type_:
        if type_ not in type_theme_map:
            raise ValueError(f"Unknown type: {type_}")
        theme = type_theme_map[type_]
        themes_types = [(theme, type_)]
    else:
        themes_types = [(type_theme_map[t], t) for t in sorted(type_theme_map.keys())]

    # Query each theme/type combination
    for theme_name, type_name in themes_types:
        dataset_path = CHANGELOG_S3_PATH_TEMPLATE.format(
            release=release, theme=theme_name, type=type_name
        )

        try:
            dataset = ds.dataset(
                dataset_path,
                filesystem=s3_fs,
                format="parquet",
                partitioning="hive",
            )

            # Stream in batches to avoid loading all rows into memory at once.
            # Use pc.value_counts() per batch for vectorised counting.
            change_counts: dict[str, int] = {}
            for batch in dataset.to_batches(columns=["change_type"]):
                if batch.num_rows == 0:
                    continue
                for item in pc.value_counts(batch.column("change_type")).to_pylist():
                    ct = item["values"]
                    change_counts[ct] = change_counts.get(ct, 0) + item["counts"]

            # Build nested structure
            if theme_name not in results:
                results[theme_name] = {}
            results[theme_name][type_name] = change_counts

        except FileNotFoundError:
            # This theme/type has no changelog data for this release
            continue
        except Exception as e:
            if "No such file" in str(e) or "does not exist" in str(e):
                continue
            raise

    return results


def _get_types_for_theme(theme: str) -> list[str]:
    """Get all feature types for a given theme.

    Args:
        theme: Overture theme name.

    Returns:
        List of feature types in the theme.
    """
    from .core import type_theme_map

    return [type_ for type_, t in type_theme_map.items() if t == theme]
