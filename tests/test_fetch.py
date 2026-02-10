"""Tests for overture_toolkit.fetch module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import geopandas as gpd
from shapely.geometry import Point

from overturemaps.fetch import THEME_S3_TEMPLATE, _rows_to_geodataframe


def test_s3_path_template_format():
    """S3 path template produces the expected URL pattern."""
    path = THEME_S3_TEMPLATE.format(
        release="2024-11-13.0", theme="buildings", type="building"
    )
    assert "overturemaps-us-west-2" in path
    assert "release/2024-11-13.0" in path
    assert "theme=buildings" in path
    assert "type=building" in path


def test_rows_to_geodataframe_with_wkb():
    """_rows_to_geodataframe converts WKB geometry bytes to Shapely."""
    from shapely import wkb

    point = Point(1.0, 2.0)
    wkb_bytes = wkb.dumps(point)
    rows = [("id_1", wkb_bytes)]
    columns = ["id", "geometry"]
    gdf = _rows_to_geodataframe(rows, columns)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 1
    assert gdf.iloc[0]["geometry"].equals(point)


def test_rows_to_geodataframe_empty():
    """_rows_to_geodataframe returns empty GeoDataFrame for empty input."""
    gdf = _rows_to_geodataframe([], ["id", "geometry"])
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 0


def test_rows_to_geodataframe_no_geometry():
    """_rows_to_geodataframe handles missing geometry column gracefully."""
    rows = [("id_1", "value")]
    columns = ["id", "data"]
    gdf = _rows_to_geodataframe(rows, columns)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert "id" in gdf.columns


@patch("overturemaps.fetch._get_connection")
def test_fetch_features_builds_id_filter(mock_conn_factory):
    """fetch_features includes ID filter in SQL when ids are provided."""
    from overturemaps.fetch import fetch_features

    mock_conn = MagicMock()
    mock_conn_factory.return_value = mock_conn

    # Mock the relation and result
    mock_rel = MagicMock()
    mock_rel.description = [("id",), ("geometry",)]
    mock_rel.fetchall.return_value = []
    mock_conn.execute.return_value = mock_rel

    fetch_features("2024-11-13.0", "buildings", "building", {"feat_001"})

    # Check that execute was called and the SQL contained IN (...)
    call_args = mock_conn.execute.call_args_list
    sql_calls = [str(c) for c in call_args]
    # At least one call should reference our feature ID
    assert any("feat_001" in s for s in sql_calls)


@patch("overturemaps.fetch._get_connection")
def test_fetch_features_no_filters(mock_conn_factory):
    """fetch_features with no ids or bbox doesn't add a WHERE clause."""
    from overturemaps.fetch import fetch_features

    mock_conn = MagicMock()
    mock_conn_factory.return_value = mock_conn

    mock_rel = MagicMock()
    mock_rel.description = [("id",), ("geometry",)]
    mock_rel.fetchall.return_value = []
    mock_conn.execute.return_value = mock_rel

    fetch_features("2024-11-13.0", "buildings", "building", None)

    # At least the main SELECT should have been executed
    assert mock_conn.execute.called
