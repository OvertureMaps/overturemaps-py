"""Functions for querying and processing the Overture Maps GERS changelog.

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
import warnings
from typing import Optional
from urllib.request import urlopen

import duckdb
import pyarrow.parquet as pq
import pyarrow.compute as pc

from .models import BBox, ChangeRecord, ChangeType

# S3 path template for the changelog Parquet files
CHANGELOG_S3_TEMPLATE = "s3://overturemaps-us-west-2/changelog/{release}/theme={theme}/type={type}/**/*.parquet"


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection with S3 anonymous access configured."""
    conn = duckdb.connect()
    conn.execute("SET s3_region='us-west-2';")
    conn.execute("SET s3_access_key_id='';")
    conn.execute("SET s3_secret_access_key='';")
    conn.execute("SET s3_session_token='';")
    # Use anonymous access for public bucket
    try:
        conn.execute(
            "CREATE OR REPLACE SECRET anon_s3 (TYPE s3, KEY_ID '', SECRET '', REGION 'us-west-2');"
        )
    except Exception:
        pass
    return conn


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

    Once changelog partitions are added to STAC (matching data partitions), this
    will automatically enable the same spatial acceleration used for data queries.

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

        # Filter by theme/type (column names may need adjustment when STAC schema is finalized)
        feature_type_filter = (pc.field("theme") == theme) & (pc.field("type") == type_)

        # Spatial filter: only include files whose bbox overlaps query bbox
        bbox_filter = (
            (pc.field("bbox", "xmin") < bbox.xmax)
            & (pc.field("bbox", "xmax") > bbox.xmin)
            & (pc.field("bbox", "ymin") < bbox.ymax)
            & (pc.field("bbox", "ymax") > bbox.ymin)
        )

        combined_filter = feature_type_filter & bbox_filter
        filtered_table = stac_table.filter(combined_filter)

        if filtered_table.num_rows > 0:
            # Extract S3 paths (schema may differ from data STAC, adjust as needed)
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
        # This is expected behavior until changelog files are added to STAC
        return None


def query_changelog_ids(
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
) -> tuple[set[str], set[str], set[str]]:
    """Query changelog and return classified ID sets directly from DuckDB.

    This is a memory-efficient alternative to query_changelog() + classify_changes().
    All aggregation happens server-side in DuckDB, returning only the ID sets.
    This can handle billions of changelog rows without exhausting memory.

    Automatically attempts to use STAC catalog for query acceleration. When changelog
    files are added to STAC, this will speed up spatial queries. Until then, transparently
    falls back to direct S3 access.

    Args:
        release: Overture release ID (e.g. "2024-11-13.0").
        theme: Overture theme name (e.g. "buildings").
        type_: Overture feature type (e.g. "building").
        bbox: Bounding box to spatially filter changes.

    Returns:
        Tuple of (ids_to_add, ids_to_modify, ids_to_delete) as sets of feature IDs.
    """
    # Try STAC first (will automatically work when changelog added to STAC)
    s3_paths = _get_changelog_files_from_stac(theme, type_, bbox, release)

    # Use STAC paths if available, otherwise fall back to full S3 path pattern
    if s3_paths is not None:
        # STAC returned specific files to query (more efficient)
        if len(s3_paths) == 0:
            # No files intersect the bbox
            return set(), set(), set()
        # Format as SQL array for DuckDB
        paths_sql = "[" + ", ".join(f"'s3://{path}'" for path in s3_paths) + "]"
    else:
        # STAC not available, use full path pattern (current behavior)
        s3_path_str = CHANGELOG_S3_TEMPLATE.format(
            release=release, theme=theme, type=type_
        )
        paths_sql = f"'{s3_path_str}'"

    conn = _get_connection()

    # Server-side aggregation: group IDs by change_type
    # This returns only the unique IDs per change type, not full records
    query = f"""
        SELECT
            change_type,
            LIST(DISTINCT id) as ids
        FROM read_parquet({paths_sql}, hive_partitioning=true)
        WHERE
            bbox.xmin <= {bbox.xmax}
            AND bbox.xmax >= {bbox.xmin}
            AND bbox.ymin <= {bbox.ymax}
            AND bbox.ymax >= {bbox.ymin}
            AND change_type != 'unchanged'
        GROUP BY change_type
    """

    rows = conn.execute(query).fetchall()

    ids_to_add: set[str] = set()
    ids_to_modify: set[str] = set()
    ids_to_delete: set[str] = set()

    for change_type, id_list in rows:
        if change_type == "added":
            ids_to_add.update(id_list)
        elif change_type == "data_changed":
            ids_to_modify.update(id_list)
        elif change_type == "removed":
            ids_to_delete.update(id_list)

    return ids_to_add, ids_to_modify, ids_to_delete


def query_changelog_ids_multi(
    release: str,
    bbox: BBox,
    theme: str | None = None,
    type_: str | None = None,
) -> dict[str, dict[str, tuple[set[str], set[str], set[str]]]]:
    """Query changelog for one or more themes/types within a bounding box.

    This function queries changelog data and returns ID sets for multiple theme/type
    combinations when theme or type are not specified.

    Automatically attempts to use STAC catalog for query acceleration when available.

    Args:
        release: Overture release ID (e.g. "2024-11-13.0").
        bbox: Bounding box to spatially filter changes.
        theme: Overture theme name (optional, defaults to all themes).
        type_: Overture feature type (optional, defaults to all types in theme).

    Returns:
        Nested dict: {theme: {type: (ids_to_add, ids_to_modify, ids_to_delete)}}.
    """
    from .core import type_theme_map

    results = {}

    # Determine which theme/type combinations to query
    if theme and type_:
        # Single theme and type specified
        themes_types = [(theme, type_)]
    elif theme:
        # Theme specified, get all types for that theme
        types = _get_types_for_theme(theme)
        themes_types = [(theme, t) for t in types]
    elif type_:
        # Type specified, get its theme
        if type_ not in type_theme_map:
            raise ValueError(f"Unknown type: {type_}")
        theme = type_theme_map[type_]
        themes_types = [(theme, type_)]
    else:
        # Neither specified, get all themes and types
        themes_types = [(type_theme_map[t], t) for t in sorted(type_theme_map.keys())]

    # Query each theme/type combination
    for theme_name, type_name in themes_types:
        try:
            ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
                release, theme_name, type_name, bbox
            )

            # Build nested structure
            if theme_name not in results:
                results[theme_name] = {}
            results[theme_name][type_name] = (ids_to_add, ids_to_modify, ids_to_delete)
        except Exception as e:
            # If a theme/type combination doesn't have changelog data, skip it
            if "No files found" in str(e) or "does not exist" in str(e):
                continue
            raise

    return results


def query_changelog(
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
) -> list[ChangeRecord]:
    """Query the changelog Parquet files for changes within a bounding box.

    .. deprecated::
        Use :func:`query_changelog_ids` instead for better memory efficiency.
        This function loads all records into memory which doesn't scale well
        for large datasets.

    Args:
        release: Overture release ID (e.g. "2024-11-13.0").
        theme: Overture theme name (e.g. "buildings").
        type_: Overture feature type (e.g. "building").
        bbox: Bounding box to spatially filter changes.

    Returns:
        List of ChangeRecord objects matching the spatial filter.
    """
    warnings.warn(
        "query_changelog() loads all records into memory and doesn't scale well. "
        "Use query_changelog_ids() instead for better memory efficiency.",
        DeprecationWarning,
        stacklevel=2,
    )
    s3_path = CHANGELOG_S3_TEMPLATE.format(release=release, theme=theme, type=type_)
    conn = _get_connection()

    query = f"""
        SELECT
            id,
            change_type,
            bbox
        FROM read_parquet('{s3_path}', hive_partitioning=true)
        WHERE
            bbox.xmin <= {bbox.xmax}
            AND bbox.xmax >= {bbox.xmin}
            AND bbox.ymin <= {bbox.ymax}
            AND bbox.ymax >= {bbox.ymin}
    """

    rows = conn.execute(query).fetchall()
    records = []
    for row in rows:
        id_, change_type_str, bbox_struct = row
        # Map new change_type values to our enum
        if change_type_str == "added":
            ct = ChangeType.added
        elif change_type_str == "removed":
            ct = ChangeType.deprecated
        elif change_type_str == "data_changed":
            ct = ChangeType.modified
        elif change_type_str == "unchanged":
            # Skip unchanged records as they don't represent actual changes
            continue
        else:
            # Default fallback
            ct = ChangeType.modified

        record_bbox = None
        if bbox_struct is not None:
            record_bbox = BBox(
                xmin=float(bbox_struct["xmin"]),
                ymin=float(bbox_struct["ymin"]),
                xmax=float(bbox_struct["xmax"]),
                ymax=float(bbox_struct["ymax"]),
            )

        records.append(
            ChangeRecord(
                id=id_,
                change_type=ct,
                successor_ids=[],  # Not available in new format
                bbox=record_bbox,
            )
        )
    return records


def _get_types_for_theme(theme: str) -> list[str]:
    """Get all feature types for a given theme.

    Args:
        theme: Overture theme name.

    Returns:
        List of feature types in the theme.
    """
    from .core import type_theme_map

    return [type_ for type_, t in type_theme_map.items() if t == theme]


def _get_all_themes() -> list[str]:
    """Get all unique theme names.

    Returns:
        List of all unique themes.
    """
    from .core import type_theme_map

    return sorted(set(type_theme_map.values()))


def summarize_changelog(
    release: str,
    theme: str | None = None,
    type_: str | None = None,
) -> dict[str, dict[str, dict[str, int]]]:
    """Return change counts by type for one or more themes without bbox filtering.

    Note: For full-dataset summaries without bbox filtering, STAC acceleration
    may not apply, but the code is prepared for future STAC optimizations.

    Args:
        release: Overture release ID.
        theme: Overture theme name (optional, defaults to all themes).
        type_: Overture feature type (optional, defaults to all types in theme).

    Returns:
        Nested dictionary: {theme: {type: {change_type: count}}}.
        If both theme and type are specified, returns single-entry nested dict.
    """
    from .core import type_theme_map

    conn = _get_connection()
    results = {}

    # Determine which theme/type combinations to query
    if theme and type_:
        # Single theme and type specified
        themes_types = [(theme, type_)]
    elif theme:
        # Theme specified, get all types for that theme
        types = _get_types_for_theme(theme)
        themes_types = [(theme, t) for t in types]
    elif type_:
        # Type specified, get its theme
        if type_ not in type_theme_map:
            raise ValueError(f"Unknown type: {type_}")
        theme = type_theme_map[type_]
        themes_types = [(theme, type_)]
    else:
        # Neither specified, get all themes and types
        themes_types = [(type_theme_map[t], t) for t in sorted(type_theme_map.keys())]

    # Query each theme/type combination
    for theme_name, type_name in themes_types:
        s3_path = CHANGELOG_S3_TEMPLATE.format(
            release=release, theme=theme_name, type=type_name
        )

        query = f"""
            SELECT change_type, COUNT(*) AS cnt
            FROM read_parquet('{s3_path}', hive_partitioning=true)
            GROUP BY change_type
            ORDER BY change_type
        """

        try:
            rows = conn.execute(query).fetchall()
            change_counts = {row[0]: row[1] for row in rows}

            # Build nested structure
            if theme_name not in results:
                results[theme_name] = {}
            results[theme_name][type_name] = change_counts
        except Exception as e:
            # If a theme/type combination doesn't have changelog data, skip it
            if "No files found" in str(e) or "does not exist" in str(e):
                continue
            raise

    return results


def classify_changes(
    records: list[ChangeRecord],
) -> tuple[set[str], set[str], set[str]]:
    """Classify change records into added, modified, and deprecated ID sets.

    .. deprecated::
        Use :func:`query_changelog_ids` instead, which performs classification
        server-side without loading records into memory.

    Args:
        records: List of ChangeRecord objects from query_changelog.

    Returns:
        Tuple of (ids_to_add, ids_to_modify, ids_to_delete).
    """
    warnings.warn(
        "classify_changes() is deprecated. Use query_changelog_ids() instead, "
        "which performs classification server-side without loading records into memory.",
        DeprecationWarning,
        stacklevel=2,
    )
    ids_to_add: set[str] = set()
    ids_to_modify: set[str] = set()
    ids_to_delete: set[str] = set()

    for record in records:
        if record.change_type == ChangeType.added:
            ids_to_add.add(record.id)
        elif record.change_type == ChangeType.modified:
            ids_to_modify.add(record.id)
        elif record.change_type == ChangeType.deprecated:
            ids_to_delete.add(record.id)

    return ids_to_add, ids_to_modify, ids_to_delete
