"""Tests for writers.py — GeoJSON writer classes and get_writer factory."""

import io
import json

from overturemaps.writers import GeoJSONSeqWriter, GeoJSONWriter, get_writer


class TestGeoJSONSeqWriter:
    def test_write_single_feature(self):
        buf = io.StringIO()
        writer = GeoJSONSeqWriter(buf)
        writer.write_feature('{"type":"Point","coordinates":[0,0]}', {"name": "test"})
        obj = json.loads(buf.getvalue().strip())
        assert obj["type"] == "Feature"
        assert obj["geometry"]["type"] == "Point"
        assert obj["properties"]["name"] == "test"

    def test_filters_none_properties(self):
        buf = io.StringIO()
        writer = GeoJSONSeqWriter(buf)
        writer.write_feature(
            '{"type":"Point","coordinates":[0,0]}',
            {"name": "test", "empty": None},
        )
        obj = json.loads(buf.getvalue().strip())
        assert "empty" not in obj["properties"]
        assert obj["properties"]["name"] == "test"

    def test_multiple_features_newline_separated(self):
        buf = io.StringIO()
        writer = GeoJSONSeqWriter(buf)
        writer.write_feature('{"type":"Point","coordinates":[0,0]}', {"id": 1})
        writer.write_feature('{"type":"Point","coordinates":[1,1]}', {"id": 2})
        lines = [line for line in buf.getvalue().splitlines() if line.strip()]
        assert len(lines) == 2
        assert json.loads(lines[0])["properties"]["id"] == 1
        assert json.loads(lines[1])["properties"]["id"] == 2

    def test_context_manager_closes_cleanly(self):
        buf = io.StringIO()
        with GeoJSONSeqWriter(buf) as writer:
            writer.write_feature('{"type":"Point","coordinates":[0,0]}', {})
        assert writer.is_open is False


class TestGeoJSONWriter:
    def test_empty_produces_valid_feature_collection(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            pass
        obj = json.loads(buf.getvalue())
        assert obj["type"] == "FeatureCollection"
        assert obj["features"] == []

    def test_single_feature(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            writer.write_feature(
                '{"type":"Point","coordinates":[0,0]}', {"name": "a"}
            )
        obj = json.loads(buf.getvalue())
        assert len(obj["features"]) == 1
        assert obj["features"][0]["type"] == "Feature"
        assert obj["features"][0]["properties"]["name"] == "a"

    def test_multiple_features_valid_json(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            writer.write_feature('{"type":"Point","coordinates":[0,0]}', {"id": 1})
            writer.write_feature('{"type":"Point","coordinates":[1,1]}', {"id": 2})
        obj = json.loads(buf.getvalue())
        assert len(obj["features"]) == 2
        assert obj["features"][1]["properties"]["id"] == 2

    def test_filters_none_properties(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            writer.write_feature(
                '{"type":"Point","coordinates":[0,0]}',
                {"name": "test", "empty": None},
            )
        obj = json.loads(buf.getvalue())
        assert "empty" not in obj["features"][0]["properties"]

    def test_context_manager_closes_cleanly(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            pass
        assert writer.is_open is False


class TestGetWriter:
    def test_geojson_returns_geojson_writer(self):
        buf = io.StringIO()
        with get_writer("geojson", buf, schema=None) as writer:
            assert isinstance(writer, GeoJSONWriter)

    def test_geojsonseq_returns_geojsonseq_writer(self):
        buf = io.StringIO()
        with get_writer("geojsonseq", buf, schema=None) as writer:
            assert isinstance(writer, GeoJSONSeqWriter)
