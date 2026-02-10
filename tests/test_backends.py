"""Tests for overture_toolkit.backends module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely.geometry import Point

from overturemaps.backends.geoparquet import GeoParquetBackend

# ---------------------------------------------------------------------------
# GeoParquetBackend tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def parquet_path(temp_dir: Path) -> Path:
    return temp_dir / "test.parquet"


@pytest.fixture()
def small_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"id": ["a", "b"], "name": ["Alice", "Bob"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )


def test_geoparquet_create_on_first_upsert(parquet_path, small_gdf):
    """GeoParquetBackend creates the file on first upsert."""
    backend = GeoParquetBackend(parquet_path)
    assert not parquet_path.exists()
    backend.upsert(small_gdf)
    assert parquet_path.exists()


def test_geoparquet_count(parquet_path, small_gdf):
    """count() reflects the number of stored features."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)
    assert backend.count() == 2


def test_geoparquet_upsert_replaces_existing(parquet_path, small_gdf):
    """Upserting an existing ID replaces rather than duplicates the row."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)

    updated = gpd.GeoDataFrame(
        {"id": ["a"], "name": ["AliceUpdated"]},
        geometry=[Point(99, 99)],
        crs="EPSG:4326",
    )
    backend.upsert(updated)

    assert backend.count() == 2
    feat = backend.get_feature("a")
    assert feat is not None
    assert feat["name"] == "AliceUpdated"


def test_geoparquet_delete(parquet_path, small_gdf):
    """delete() removes the specified IDs."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)
    backend.delete({"a"})
    assert backend.count() == 1
    assert backend.get_feature("a") is None


def test_geoparquet_delete_empty_set(parquet_path, small_gdf):
    """delete() with empty set does nothing."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)
    backend.delete(set())
    assert backend.count() == 2


def test_geoparquet_get_feature(parquet_path, small_gdf):
    """get_feature returns correct attributes for existing ID."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)
    feat = backend.get_feature("b")
    assert feat is not None
    assert feat["name"] == "Bob"


def test_geoparquet_get_feature_missing(parquet_path):
    """get_feature returns None for non-existent ID."""
    backend = GeoParquetBackend(parquet_path)
    assert backend.get_feature("nonexistent") is None


def test_geoparquet_upsert_empty_does_nothing(parquet_path):
    """Upserting an empty GeoDataFrame does not create a file."""
    backend = GeoParquetBackend(parquet_path)
    empty = gpd.GeoDataFrame()
    backend.upsert(empty)
    assert not parquet_path.exists()


def test_geoparquet_upsert_handles_crs_mismatch(parquet_path):
    """Upserting with different CRS representations handles conversion correctly."""
    backend = GeoParquetBackend(parquet_path)

    # Initial data with EPSG:4326 (WGS 84)
    initial = gpd.GeoDataFrame(
        {"id": ["a", "b"], "name": ["Alice", "Bob"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs="EPSG:4326",
    )
    backend.upsert(initial)

    # New data with CRS84 (WGS 84 longitude/latitude order)
    # This simulates what happens when data comes from different sources
    new_data = gpd.GeoDataFrame(
        {"id": ["c"], "name": ["Charlie"]},
        geometry=[Point(2, 2)],
        crs="OGC:CRS84",  # Alternative representation of WGS 84
    )

    # This should not raise a CRS mismatch error
    backend.upsert(new_data)
    assert backend.count() == 3


def test_geoparquet_upsert_replaces_all_existing(parquet_path, small_gdf):
    """Upserting all existing IDs replaces the entire dataset without warnings."""
    backend = GeoParquetBackend(parquet_path)
    backend.upsert(small_gdf)
    assert backend.count() == 2

    # Replace all existing rows with completely new data
    replacement = gpd.GeoDataFrame(
        {"id": ["a", "b"], "name": ["NewAlice", "NewBob"]},
        geometry=[Point(10, 10), Point(20, 20)],
        crs="EPSG:4326",
    )
    backend.upsert(replacement)

    # Should still have 2 rows with updated data
    assert backend.count() == 2
    feat_a = backend.get_feature("a")
    assert feat_a["name"] == "NewAlice"


# ---------------------------------------------------------------------------
# PostGISBackend tests (mocked SQLAlchemy)
# ---------------------------------------------------------------------------


def _extract_sql_texts(mock_conn) -> list[str]:
    """Extract SQL text strings from SQLAlchemy TextClause mock calls."""
    texts = []
    for call_args in mock_conn.execute.call_args_list:
        args = call_args[0]
        if args:
            arg = args[0]
            # TextClause objects have a .text attribute
            if hasattr(arg, "text"):
                texts.append(arg.text)
            else:
                texts.append(str(arg))
    return texts


@patch("overturemaps.backends.postgis.create_engine")
def test_postgis_ensures_table_on_init(mock_create_engine):
    """PostGISBackend creates the table during __init__."""
    from overturemaps.backends.postgis import PostGISBackend

    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    PostGISBackend("postgresql://localhost/test", "features")

    # Should have executed CREATE TABLE
    assert mock_conn.execute.called
    executed_sqls = _extract_sql_texts(mock_conn)
    assert any("CREATE TABLE" in s for s in executed_sqls)


@patch("overturemaps.backends.postgis.create_engine")
def test_postgis_delete_sends_correct_sql(mock_create_engine):
    """PostGISBackend.delete builds a DELETE WHERE id IN (...) statement."""
    from overturemaps.backends.postgis import PostGISBackend

    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    backend = PostGISBackend("postgresql://localhost/test", "features")
    mock_conn.execute.reset_mock()

    backend.delete({"id_1", "id_2"})

    executed_sqls = _extract_sql_texts(mock_conn)
    combined = " ".join(executed_sqls)
    assert "DELETE" in combined
    assert "id_1" in combined or "id_2" in combined


@patch("overturemaps.backends.postgis.create_engine")
def test_postgis_count_query(mock_create_engine):
    """PostGISBackend.count executes SELECT COUNT(*)."""
    from overturemaps.backends.postgis import PostGISBackend

    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_begin_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_begin_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mock_select_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(
        return_value=mock_select_conn
    )
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    mock_result = MagicMock()
    mock_result.scalar.return_value = 42
    mock_select_conn.execute.return_value = mock_result

    backend = PostGISBackend("postgresql://localhost/test", "features")
    assert backend.count() == 42

    executed = _extract_sql_texts(mock_select_conn)
    assert any("COUNT" in s for s in executed)
