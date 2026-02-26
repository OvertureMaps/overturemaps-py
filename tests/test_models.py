"""Tests for the models module."""

from overturemaps.models import BBox, Backend, PipelineState


def test_bbox_as_tuple():
    """Test BBox.as_tuple()."""
    bbox = BBox(xmin=-97.8, ymin=30.2, xmax=-97.6, ymax=30.4)
    assert bbox.as_tuple() == (-97.8, 30.2, -97.6, 30.4)


def test_bbox_as_dict():
    """Test BBox.as_dict()."""
    bbox = BBox(xmin=-97.8, ymin=30.2, xmax=-97.6, ymax=30.4)
    result = bbox.as_dict()
    assert result == {
        "xmin": -97.8,
        "ymin": 30.2,
        "xmax": -97.6,
        "ymax": 30.4,
    }


def test_bbox_from_dict():
    """Test BBox.from_dict()."""
    data = {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4}
    bbox = BBox.from_dict(data)
    assert bbox.xmin == -97.8
    assert bbox.ymin == 30.2
    assert bbox.xmax == -97.6
    assert bbox.ymax == 30.4


def test_backend_enum():
    """Test Backend enum values."""
    assert Backend.geojson == "geojson"
    assert Backend.geojsonseq == "geojsonseq"
    assert Backend.geoparquet == "geoparquet"
    assert str(Backend.geojson) == "geojson"


def test_pipeline_state_serialization():
    """Test PipelineState serialization and deserialization."""
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

    # Serialize to dict
    data = state.as_dict()
    assert data["last_release"] == "2024-11-13.0"
    assert data["theme"] == "buildings"
    assert data["type"] == "building"
    assert data["backend"] == "geoparquet"
    assert data["bbox"]["xmin"] == -97.8

    # Deserialize from dict
    restored = PipelineState.from_dict(data)
    assert restored.last_release == state.last_release
    assert restored.theme == state.theme
    assert restored.type == state.type
    assert restored.backend == state.backend
    assert restored.bbox.xmin == state.bbox.xmin
    assert restored.output == state.output


def test_pipeline_state_serialization_with_null_bbox():
    """Test PipelineState serialization and deserialization with null bbox."""
    state = PipelineState(
        last_release="2024-11-13.0",
        last_run="2024-11-13T12:00:00Z",
        theme="buildings",
        type="building",
        bbox=None,
        backend=Backend.geojson,
        output="/tmp/output.geojson",
    )

    data = state.as_dict()
    assert data["bbox"] is None

    restored = PipelineState.from_dict(data)
    assert restored.bbox is None
    assert restored.last_release == state.last_release
    assert restored.output == state.output
