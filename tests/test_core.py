"""Tests for pure (no-network) functions in core.py."""

import pyarrow as pa
import pytest

from overturemaps.core import (
    _binary_search_manifest,
    _coerce_bbox,
    _dataset_path,
    geoarrow_schema_adapter,
    get_all_overture_types,
    type_theme_map,
)
from overturemaps.models import BBox


class TestCoerceBbox:
    def test_none_returns_none(self):
        assert _coerce_bbox(None) is None

    def test_bbox_instance_passthrough(self):
        bbox = BBox(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0)
        assert _coerce_bbox(bbox) is bbox

    def test_from_tuple(self):
        result = _coerce_bbox((-71.1, 42.3, -71.0, 42.4))
        assert isinstance(result, BBox)
        assert result.xmin == -71.1
        assert result.ymin == 42.3
        assert result.xmax == -71.0
        assert result.ymax == 42.4

    def test_from_list(self):
        result = _coerce_bbox([-71.1, 42.3, -71.0, 42.4])
        assert isinstance(result, BBox)
        assert result.ymin == 42.3

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError):
            _coerce_bbox((1.0, 2.0, 3.0))


class TestDatasetPath:
    def test_building_contains_theme_and_type(self):
        path = _dataset_path("building", "2025-01-22.0")
        assert "buildings" in path
        assert "building" in path
        assert "2025-01-22.0" in path

    def test_path_starts_with_s3_bucket(self):
        path = _dataset_path("place", "2025-01-22.0")
        assert path.startswith("overturemaps-us-west-2/release/")
        assert "places" in path
        assert "place" in path


class TestBinarySearchManifest:
    def test_first_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "aaa") == "file1.parquet"

    def test_middle_file(self):
        manifest = [
            ("file1.parquet", "bbb"),
            ("file2.parquet", "ddd"),
            ("file3.parquet", "fff"),
        ]
        assert _binary_search_manifest(manifest, "ccc") == "file2.parquet"

    def test_exact_max_id_matches_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "bbb") == "file1.parquet"

    def test_last_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "ddd") == "file2.parquet"

    def test_not_found_returns_none(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "zzz") is None

    def test_empty_manifest_returns_none(self):
        assert _binary_search_manifest([], "aaa") is None


class TestGetAllOvertureTypes:
    def test_returns_nonempty_list(self):
        result = get_all_overture_types()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_includes_core_types(self):
        types = get_all_overture_types()
        for t in ("building", "place", "segment", "water", "land"):
            assert t in types

    def test_matches_type_theme_map_keys(self):
        assert set(get_all_overture_types()) == set(type_theme_map.keys())


class TestGeoarrowSchemaAdapter:
    def test_adds_extension_metadata_to_geometry(self):
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("geometry", pa.large_binary())]
        )
        result = geoarrow_schema_adapter(schema)
        geom_field = result.field("geometry")
        assert geom_field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"

    def test_non_geometry_fields_unchanged(self):
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("geometry", pa.large_binary())]
        )
        result = geoarrow_schema_adapter(schema)
        assert result.field("id").type == pa.string()
        assert result.field("id").metadata is None
