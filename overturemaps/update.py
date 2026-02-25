"""Incremental update functionality using pyarrow.

This module implements the core update logic following jwass's feedback:
1. Read existing local file as pyarrow Table
2. Query changelog for IDs (added, modified, removed)
3. Fetch new/modified features from S3 using pyarrow.dataset
4. Filter out removed+modified from existing using pyarrow.compute.is_in
5. Concatenate kept features + new features
6. Write back using appropriate writer

No backend abstraction - just direct pyarrow table operations.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import shapely.wkb

from .models import BBox, Backend
from .changelog import query_changelog_ids
from .core import type_theme_map


def fetch_features_pyarrow(
    release: str,
    theme: str,
    type_: str,
    ids: set[str],
    bbox: Optional[BBox] = None,
) -> pa.Table:
    """Fetch features from S3 using pyarrow by ID.

    Args:
        release: Overture release ID.
        theme: Overture theme name.
        type_: Overture feature type.
        ids: Set of feature IDs to fetch.
        bbox: Optional bounding box filter.

    Returns:
        PyArrow Table with the fetched features.
    """
    if not ids:
        # Return empty table with no schema
        return None

    s3_fs = fs.S3FileSystem(anonymous=True, region="us-west-2")
    dataset_path = f"overturemaps-us-west-2/release/{release}/theme={theme}/type={type_}/"

    dataset = ds.dataset(
        dataset_path,
        filesystem=s3_fs,
        format="parquet",
        partitioning="hive",
    )

    # Build filter expression
    filter_expr = pc.is_in(pc.field("id"), value_set=pa.array(list(ids)))

    if bbox is not None:
        bbox_filter = (
            (pc.field("bbox", "xmin") < bbox.xmax)
            & (pc.field("bbox", "xmax") > bbox.xmin)
            & (pc.field("bbox", "ymin") < bbox.ymax)
            & (pc.field("bbox", "ymax") > bbox.ymin)
        )
        filter_expr = filter_expr & bbox_filter

    return dataset.to_table(filter=filter_expr)


def read_local_file(path: Path, backend: Backend) -> Optional[pa.Table]:
    """Read local file into a pyarrow Table.

    Args:
        path: Path to the local file.
        backend: Backend type (geojson, geojsonseq, geoparquet).

    Returns:
        PyArrow Table with the data, or None if file doesn't exist.
    """
    if not path.exists():
        return None

    if backend == Backend.geoparquet:
        return pq.read_table(path)
    elif backend == Backend.geojsonseq:
        return _read_geojsonseq(path)
    elif backend == Backend.geojson:
        return _read_geojson(path)
    else:
        raise ValueError(f"Unsupported backend: {backend}")


def _read_geojsonseq(path: Path) -> pa.Table:
    """Read GeoJSON Sequence file into a pyarrow Table.
    
    This is a simplified implementation. For production use, consider using
    a proper GeoJSON reader or geopandas.
    """
    import json

    features = []
    with open(path, "r") as f:
        for line in f:
            if line.strip():
                features.append(json.loads(line))

    # Convert to pyarrow (simplified - would need proper geometry handling)
    # For now, raise an error suggesting geoparquet for updates
    raise NotImplementedError(
        "GeoJSON Sequence updates not yet implemented. "
        "Please use geoparquet format for incremental updates."
    )


def _read_geojson(path: Path) -> pa.Table:
    """Read GeoJSON file into a pyarrow Table.
    
    This is a simplified implementation. For production use, consider using
    a proper GeoJSON reader or geopandas.
    """
    import json

    with open(path, "r") as f:
        data = json.load(f)

    # For now, raise an error suggesting geoparquet for updates
    raise NotImplementedError(
        "GeoJSON updates not yet implemented. "
        "Please use geoparquet format for incremental updates."
    )


def write_local_file(
    table: pa.Table, path: Path, backend: Backend, schema: Optional[pa.Schema] = None
) -> None:
    """Write pyarrow Table to local file.

    Args:
        table: PyArrow Table to write.
        path: Path to write to.
        backend: Backend type.
        schema: Optional schema (used for geoparquet metadata).
    """
    if backend == Backend.geoparquet:
        # Use the schema from the original file if available
        if schema is not None:
            table = table.cast(schema)
        pq.write_table(table, path)
    else:
        raise NotImplementedError(
            f"Writing to {backend} not yet implemented. "
            "Please use geoparquet format for incremental updates."
        )


def apply_update(
    output_path: Path,
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
    backend: Backend,
    from_release: Optional[str] = None,
) -> dict[str, int]:
    """Apply incremental update to a local file.

    Args:
        output_path: Path to the local file to update.
        release: Target release to update to.
        theme: Overture theme name.
        type_: Overture feature type.
        bbox: Bounding box for spatial filtering.
        backend: Storage backend type.
        from_release: Source release (optional, auto-detected from file).

    Returns:
        Dict with update statistics: {
            "added": int,
            "modified": int,
            "deleted": int,
            "final_count": int
        }
    """
    # Read existing data
    existing = read_local_file(output_path, backend)

    if existing is None:
        raise FileNotFoundError(f"File not found: {output_path}")

    original_count = existing.num_rows
    original_schema = existing.schema

    # Query changelog for IDs
    ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
        release, theme, type_, bbox
    )

    # Fetch new and modified features
    ids_to_fetch = ids_to_add | ids_to_modify

    if ids_to_fetch:
        new_features = fetch_features_pyarrow(release, theme, type_, ids_to_fetch, bbox)
    else:
        new_features = None

    # Filter out removed + modified from existing
    # (modified will be replaced with fresh versions from new_features)
    ids_to_remove = ids_to_delete | ids_to_modify

    if ids_to_remove:
        existing_ids = existing.column("id")
        # Create mask: keep rows where ID is NOT in ids_to_remove
        mask = pc.invert(pc.is_in(existing_ids, value_set=pa.array(list(ids_to_remove))))
        kept = existing.filter(mask)
    else:
        kept = existing

    # Concatenate kept + new
    if new_features is not None and new_features.num_rows > 0:
        # Ensure schemas match (use original schema)
        if new_features.schema != original_schema:
            # Align columns
            new_features = new_features.select(
                [col for col in original_schema.names if col in new_features.schema.names]
            )
        updated = pa.concat_tables([kept, new_features])
    else:
        updated = kept

    # Write back
    write_local_file(updated, output_path, backend, schema=original_schema)

    return {
        "added": len(ids_to_add),
        "modified": len(ids_to_modify),
        "deleted": len(ids_to_delete),
        "original_count": original_count,
        "final_count": updated.num_rows,
    }
