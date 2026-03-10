"""Tests for the state module."""

import json
import tempfile
from pathlib import Path

from overturemaps.models import BBox, Backend, PipelineState
from overturemaps.state import get_state_path, load_state, save_state


def test_get_state_path():
    """Test get_state_path() generates correct path."""
    assert get_state_path("/tmp/output.parquet") == Path("/tmp/output.parquet.state")
    assert get_state_path("data.geojson") == Path("data.geojson.state")


def test_save_and_load_state():
    """Test saving and loading pipeline state."""
    bbox = BBox(xmin=-97.8, ymin=30.2, xmax=-97.6, ymax=30.4)
    state = PipelineState(
        last_release="2024-11-13.0",
        last_run="2024-11-13T12:00:00Z",
        theme="buildings",
        type="building",
        bbox=bbox,
        backend=Backend.geoparquet,
        output="/tmp/output.parquet",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "test.state"

        # Save state
        save_state(state, state_path)
        assert state_path.exists()

        # Load state
        loaded = load_state(state_path)
        assert loaded is not None
        assert loaded.last_release == "2024-11-13.0"
        assert loaded.theme == "buildings"
        assert loaded.type == "building"
        assert loaded.backend == Backend.geoparquet
        assert loaded.bbox.xmin == -97.8
        assert loaded.output == "/tmp/output.parquet"


def test_load_state_nonexistent():
    """Test loading state from non-existent file returns None."""
    result = load_state("/nonexistent/path/state.json")
    assert result is None


def test_load_state_invalid_json():
    """Test loading state from invalid JSON returns None."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".state", delete=False) as f:
        f.write("invalid json {")
        temp_path = f.name

    try:
        result = load_state(temp_path)
        assert result is None
    finally:
        Path(temp_path).unlink()


def test_save_state_creates_parent_directory():
    """Test that save_state creates parent directories if needed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "nested" / "path" / "state.json"

        bbox = BBox(xmin=-97.8, ymin=30.2, xmax=-97.6, ymax=30.4)
        state = PipelineState(
            last_release="2024-11-13.0",
            last_run="2024-11-13T12:00:00Z",
            theme="buildings",
            type="building",
            bbox=bbox,
            backend=Backend.geoparquet,
            output="/tmp/output.parquet",
        )

        save_state(state, state_path)
        assert state_path.exists()
        assert state_path.parent.exists()


def test_save_and_load_state_with_null_bbox():
    """Test saving and loading pipeline state when bbox is null."""
    state = PipelineState(
        last_release="2024-11-13.0",
        last_run="2024-11-13T12:00:00Z",
        theme="buildings",
        type="building",
        bbox=None,
        backend=Backend.geojson,
        output="/tmp/output.geojson",
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "test-null-bbox.state"

        save_state(state, state_path)
        assert state_path.exists()

        with open(state_path, "r") as f:
            raw = json.load(f)
        assert raw["bbox"] is None

        loaded = load_state(state_path)
        assert loaded is not None
        assert loaded.bbox is None
        assert loaded.output == "/tmp/output.geojson"
