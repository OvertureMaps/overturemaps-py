"""A tiny fake STAC index for tests that make no network requests."""

import io

import pyarrow as pa
import pyarrow.parquet as pq

RELEASE = "2026-10-21.0"
SF_BBOX = (-122.42, 37.77, -122.40, 37.785)
OCEAN_BBOX = (-40.0, 0.0, -39.9, 0.1)
WEST_FILE = f"overturemaps-us-west-2/release/{RELEASE}/theme=buildings/type=building/west.parquet"


def stac_index_bytes() -> bytes:
    """A tiny STAC index: two building files in North America and one global place file."""

    def item(collection, path, bbox):
        return {
            "type": "Feature",
            "collection": collection,
            "bbox": dict(zip(("xmin", "ymin", "xmax", "ymax"), bbox)),
            "assets": {"aws": {"alternate": {"s3": {"href": f"s3://{path}"}}}},
        }

    table = pa.Table.from_pylist(
        [
            item("building", WEST_FILE, (-125.0, 30.0, -110.0, 45.0)),
            item("building", WEST_FILE.replace("west", "east"), (-80.0, 35.0, -70.0, 45.0)),
            item("place", WEST_FILE.replace("building", "place"), (-180.0, -90.0, 180.0, 90.0)),
        ]
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()
