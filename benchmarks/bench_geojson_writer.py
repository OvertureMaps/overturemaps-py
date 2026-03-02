"""
Benchmarks for the GeoJSON writing pipeline.

Exercises the write_batch() hot loop to establish a baseline for each stage:
  Arrow->Python, WKB decode, geo_interface conversion, json.dumps.

Run all benchmarks:
    pytest benchmarks/ -v

Run only benchmarks (skip normal tests):
    pytest benchmarks/ -v --benchmark-only

Save results to JSON for later comparison:
    pytest benchmarks/ --benchmark-json=results/baseline.json
"""
import io
import json
import random

import pyarrow as pa
import pytest
import shapely
import shapely.wkb
from shapely.geometry import Point, Polygon

from overturemaps.cli import GeoJSONSeqWriter


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

_BBOX_TYPE = pa.struct(
    [
        pa.field("xmin", pa.float64()),
        pa.field("ymin", pa.float64()),
        pa.field("xmax", pa.float64()),
        pa.field("ymax", pa.float64()),
    ]
)

_NAMES_TYPE = pa.struct([pa.field("primary", pa.string())])


def _point_wkb(lon, lat):
    return shapely.wkb.dumps(Point(lon, lat))


def _polygon_wkb(cx, cy, r=0.01):
    coords = [
        (cx - r, cy - r),
        (cx + r, cy - r),
        (cx + r, cy + r),
        (cx - r, cy + r),
        (cx - r, cy - r),
    ]
    return shapely.wkb.dumps(Polygon(coords))


def _make_batch(n_rows, geom_fn, seed=42):
    """Return a PyArrow RecordBatch that mimics an Overture Parquet partition."""
    rng = random.Random(seed)
    lons = [rng.uniform(-180, 180) for _ in range(n_rows)]
    lats = [rng.uniform(-90, 90) for _ in range(n_rows)]

    return pa.record_batch(
        {
            "id": pa.array(
                [f"08f{i:015x}" for i in range(n_rows)], type=pa.string()
            ),
            "geometry": pa.array(
                [geom_fn(lon, lat) for lon, lat in zip(lons, lats)],
                type=pa.binary(),
            ),
            "bbox": pa.array(
                [
                    {
                        "xmin": lon - 0.01,
                        "ymin": lat - 0.01,
                        "xmax": lon + 0.01,
                        "ymax": lat + 0.01,
                    }
                    for lon, lat in zip(lons, lats)
                ],
                type=_BBOX_TYPE,
            ),
            "confidence": pa.array(
                [rng.random() for _ in range(n_rows)], type=pa.float64()
            ),
            "names": pa.array(
                [{"primary": f"feature_{i}"} for i in range(n_rows)],
                type=_NAMES_TYPE,
            ),
        }
    )


# ---------------------------------------------------------------------------
# Fixtures  (session-scoped so batch construction is outside timing)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def batch_1k_points():
    return _make_batch(1_000, _point_wkb)


@pytest.fixture(scope="session")
def batch_10k_points():
    return _make_batch(10_000, _point_wkb)


@pytest.fixture(scope="session")
def batch_1k_polygons():
    return _make_batch(1_000, _polygon_wkb)


@pytest.fixture(scope="session")
def batch_10k_polygons():
    return _make_batch(10_000, _polygon_wkb)


# ---------------------------------------------------------------------------
# Full pipeline benchmarks
# (Arrow batch -> GeoJSON newline-delimited text, written to StringIO)
# ---------------------------------------------------------------------------


def _make_writer():
    """GeoJSONSeqWriter backed by an in-memory sink (removes disk I/O from timing)."""
    return GeoJSONSeqWriter(io.StringIO())


def test_full_pipeline_1k_points(benchmark, batch_1k_points):
    """Full write_batch pipeline: 1 000 point rows -> GeoJSONSeq."""
    writer = _make_writer()
    benchmark(writer.write_batch, batch_1k_points)


def test_full_pipeline_10k_points(benchmark, batch_10k_points):
    """Full write_batch pipeline: 10 000 point rows -> GeoJSONSeq."""
    writer = _make_writer()
    benchmark(writer.write_batch, batch_10k_points)


def test_full_pipeline_1k_polygons(benchmark, batch_1k_polygons):
    """Full write_batch pipeline: 1 000 polygon rows -> GeoJSONSeq."""
    writer = _make_writer()
    benchmark(writer.write_batch, batch_1k_polygons)


def test_full_pipeline_10k_polygons(benchmark, batch_10k_polygons):
    """Full write_batch pipeline: 10 000 polygon rows -> GeoJSONSeq."""
    writer = _make_writer()
    benchmark(writer.write_batch, batch_10k_polygons)


# ---------------------------------------------------------------------------
# Isolated stage benchmarks
# Each stage is timed independently using pre-computed inputs so only the
# operation under test contributes to the measurement.
# ---------------------------------------------------------------------------


def test_stage_to_pylist(benchmark, batch_10k_points):
    """Stage 1 — Arrow batch -> Python list of dicts (batch.to_pylist)."""
    benchmark(batch_10k_points.to_pylist)


def test_stage_wkb_loads_loop(benchmark, batch_10k_points):
    """Stage 2 — WKB decode: shapely.wkb.loads() called once per row in a loop."""
    geom_bytes = batch_10k_points.column("geometry").to_pylist()

    def run():
        return [shapely.wkb.loads(b) for b in geom_bytes]

    benchmark(run)


def test_stage_geo_interface_loop(benchmark, batch_10k_points):
    """Stage 3 — Shapely geometry -> __geo_interface__ dict, once per row."""
    geom_bytes = batch_10k_points.column("geometry").to_pylist()
    geoms = [shapely.wkb.loads(b) for b in geom_bytes]

    def run():
        return [g.__geo_interface__ for g in geoms]

    benchmark(run)


def test_stage_json_dumps_loop(benchmark, batch_10k_points):
    """Stage 4 — json.dumps() once per GeoJSON feature."""
    geom_bytes = batch_10k_points.column("geometry").to_pylist()
    geoms = [shapely.wkb.loads(b) for b in geom_bytes]
    features = [
        {"type": "Feature", "geometry": g.__geo_interface__, "properties": {}}
        for g in geoms
    ]

    def run():
        return [json.dumps(f, separators=(",", ":")) for f in features]

    benchmark(run)
