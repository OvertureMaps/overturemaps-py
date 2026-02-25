"""Tests for the update module."""

import tempfile
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from overturemaps.models import BBox, Backend
from overturemaps.update import apply_update, read_local_file, write_local_file
from overturemaps.releases import get_latest_release


def create_test_parquet(path: Path, ids: list[str]) -> None:
    """Create a test geoparquet file with given IDs."""
    # Create a simple schema
    schema = pa.schema([
        ("id", pa.string()),
        ("name", pa.string()),
        ("geometry", pa.binary()),
        ("bbox", pa.struct([
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ])),
    ])
    
    # Create sample data
    data = {
        "id": ids,
        "name": [f"Feature {i}" for i in range(len(ids))],
        "geometry": [b"" for _ in ids],  # Empty WKB for testing
        "bbox": [
            {"xmin": -97.8, "ymin": 30.2, "xmax": -97.7, "ymax": 30.3}
            for _ in ids
        ],
    }
    
    table = pa.table(data, schema=schema)
    pq.write_table(table, path)


def test_read_local_file_geoparquet():
    """Test reading a local geoparquet file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.parquet"
        create_test_parquet(test_file, ["id1", "id2", "id3"])
        
        table = read_local_file(test_file, Backend.geoparquet)
        assert table is not None
        assert table.num_rows == 3
        assert "id" in table.column_names


def test_read_local_file_nonexistent():
    """Test reading a non-existent file returns None."""
    result = read_local_file(Path("/nonexistent/file.parquet"), Backend.geoparquet)
    assert result is None


def test_write_local_file_geoparquet():
    """Test writing a pyarrow table to geoparquet."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.parquet"
        
        # Create a simple table
        schema = pa.schema([
            ("id", pa.string()),
            ("name", pa.string()),
        ])
        data = {
            "id": ["id1", "id2"],
            "name": ["Feature 1", "Feature 2"],
        }
        table = pa.table(data, schema=schema)
        
        write_local_file(table, test_file, Backend.geoparquet)
        assert test_file.exists()
        
        # Read it back
        loaded = pq.read_table(test_file)
        assert loaded.num_rows == 2


@pytest.mark.integration
def test_apply_update_geoparquet():
    """Integration test for apply_update with geoparquet.
    
    This test requires network access to S3.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        test_file = Path(tmpdir) / "test.parquet"
        
        # Create a test file with some IDs
        # Note: In a real scenario, these would be actual feature IDs from Overture
        create_test_parquet(test_file, ["test_id_1", "test_id_2"])
        
        latest = get_latest_release()
        bbox = BBox(xmin=-97.75, ymin=30.25, xmax=-97.74, ymax=30.26)
        
        # This will attempt to fetch real changelog data
        # In practice, we'd want to mock this or use known test data
        try:
            stats = apply_update(
                test_file,
                latest,
                "buildings",
                "building",
                bbox,
                Backend.geoparquet,
            )
            
            # Check that stats were returned
            assert "added" in stats
            assert "modified" in stats
            assert "deleted" in stats
            assert "final_count" in stats
            
        except Exception as e:
            # If changelog data doesn't exist for this bbox, that's ok
            if "No such file" in str(e) or "does not exist" in str(e):
                pytest.skip("No changelog data for test bbox")
            else:
                raise


def test_read_geojson_not_implemented():
    """Test that GeoJSON reading raises NotImplementedError."""
    with tempfile.NamedTemporaryFile(suffix=".geojson", mode="w", delete=False) as f:
        f.write('{"type": "FeatureCollection", "features": []}')
        temp_path = Path(f.name)
    
    try:
        with pytest.raises(NotImplementedError):
            read_local_file(temp_path, Backend.geojson)
    finally:
        temp_path.unlink()


def test_read_geojsonseq_not_implemented():
    """Test that GeoJSON Sequence reading raises NotImplementedError."""
    with tempfile.NamedTemporaryFile(suffix=".geojsonl", mode="w", delete=False) as f:
        f.write('{"type": "Feature"}\n')
        temp_path = Path(f.name)
    
    try:
        with pytest.raises(NotImplementedError):
            read_local_file(temp_path, Backend.geojsonseq)
    finally:
        temp_path.unlink()
