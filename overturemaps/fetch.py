"""Functions for fetching Overture Maps feature data from S3."""

from __future__ import annotations

import duckdb
import geopandas as gpd
import pandas as pd
from shapely import wkb

from .models import BBox

# S3 path template for Overture theme data
THEME_S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme={theme}/type={type}/*.parquet"
)


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection with S3 anonymous access configured."""
    conn = duckdb.connect()
    conn.execute("SET s3_region='us-west-2';")
    conn.execute("SET s3_access_key_id='';")
    conn.execute("SET s3_secret_access_key='';")
    conn.execute("SET s3_session_token='';")
    try:
        conn.execute(
            "CREATE OR REPLACE SECRET anon_s3 (TYPE s3, KEY_ID '', SECRET '', REGION 'us-west-2');"
        )
    except Exception:
        # Some DuckDB versions or configurations may not support SECRETs; the
        # connection is still usable with the S3 settings above, so we can safely
        # ignore failures when creating this optional secret.
        pass
    return conn


def _rows_to_geodataframe(rows: list, columns: list[str]) -> gpd.GeoDataFrame:
    """Convert DuckDB result rows into a GeoDataFrame.

    Assumes a 'geometry' column containing WKB bytes.

    Args:
        rows: List of row tuples from DuckDB.
        columns: Column names corresponding to row positions.

    Returns:
        GeoDataFrame with geometry parsed from WKB.
    """
    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries([], crs="EPSG:4326"))

    if "geometry" in df.columns:
        df["geometry"] = df["geometry"].apply(
            lambda g: wkb.loads(bytes(g)) if g is not None else None
        )
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    else:
        gdf = gpd.GeoDataFrame(df)
    return gdf


def fetch_features(
    release: str,
    theme: str,
    type_: str,
    ids: set[str] | None,
    bbox: BBox | None = None,
) -> gpd.GeoDataFrame:
    """Fetch specific features by ID and/or bounding box from S3.

    Args:
        release: Overture release ID.
        theme: Overture theme name.
        type_: Overture feature type.
        ids: Set of feature IDs to fetch, or None to skip ID filtering.
        bbox: Optional bounding box for spatial filtering.

    Returns:
        GeoDataFrame containing matched features.
    """
    s3_path = THEME_S3_TEMPLATE.format(release=release, theme=theme, type=type_)
    conn = _get_connection()

    # Build WHERE clause
    conditions = []
    if ids:
        id_list = ", ".join(f"'{i}'" for i in ids)
        conditions.append(f"id IN ({id_list})")
    if bbox is not None:
        conditions.append(
            f"bbox.xmin <= {bbox.xmax} AND bbox.xmax >= {bbox.xmin} "
            f"AND bbox.ymin <= {bbox.ymax} AND bbox.ymax >= {bbox.ymin}"
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT * EXCLUDE (geometry), geometry
        FROM read_parquet('{s3_path}', hive_partitioning=true)
        {where_clause}
    """

    rel = conn.execute(query)
    columns = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    return _rows_to_geodataframe(rows, columns)


def fetch_all_features(
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
) -> gpd.GeoDataFrame:
    """Fetch all features within a bounding box for initial dataset creation.

    Args:
        release: Overture release ID.
        theme: Overture theme name.
        type_: Overture feature type.
        bbox: Bounding box to spatially filter features.

    Returns:
        GeoDataFrame containing all features within the bbox.
    """
    return fetch_features(release, theme, type_, ids=None, bbox=bbox)
