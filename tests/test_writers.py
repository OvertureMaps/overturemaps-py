"""Tests for writers.py — GeoJSON writer classes and get_writer factory."""

import builtins
import io
import json
import os

import pytest

from overturemaps.writers import GeoJSONSeqWriter, GeoJSONWriter, get_writer

# Characters that fail under cp932 (Japanese Windows) or charmap (Western Windows).
_NON_ASCII_CHARS = "Ć Č \u2013 \u0106"


@pytest.fixture()
def narrow_open(monkeypatch):
    """Simulate a platform where open() defaults to a narrow encoding (e.g. cp932/ascii).

    Any open() call that doesn't explicitly pass encoding= will get 'ascii',
    causing UnicodeEncodeError on non-ASCII content — exactly the failure
    reported in issue #113. Writers that hard-code encoding='utf-8' are immune.
    """
    _real_open = builtins.open

    def _narrow(file, mode="r", **kwargs):
        if "b" not in mode and "encoding" not in kwargs:
            kwargs["encoding"] = "ascii"
        return _real_open(file, mode, **kwargs)

    monkeypatch.setattr(builtins, "open", _narrow)
    return _narrow


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
        assert json.loads(lines[0])["id"] == 1
        assert json.loads(lines[1])["id"] == 2

    def test_writes_top_level_id_not_properties(self):
        buf = io.StringIO()
        writer = GeoJSONSeqWriter(buf)
        writer.write_feature('{"type":"Point","coordinates":[0,0]}', {"id": "abc", "name": "n"})
        obj = json.loads(buf.getvalue().strip())
        assert obj["id"] == "abc"
        assert "id" not in obj["properties"]
        assert obj["properties"]["name"] == "n"

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
        assert obj["features"][1]["id"] == 2

    def test_writes_top_level_id_not_properties(self):
        buf = io.StringIO()
        with GeoJSONWriter(buf) as writer:
            writer.write_feature(
                '{"type":"Point","coordinates":[0,0]}',
                {"id": "abc", "name": "n"},
            )
        feature = json.loads(buf.getvalue())["features"][0]
        assert feature["id"] == "abc"
        assert "id" not in feature["properties"]
        assert feature["properties"]["name"] == "n"

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


class TestNarrowEncodingFile:
    """Regression tests for issue #113 — UnicodeEncodeError on Windows.

    The narrow_open fixture patches builtins.open so any call without an
    explicit encoding= gets 'ascii', reproducing the cp932/charmap failure.
    Tests here must still pass: writers must hard-code encoding='utf-8'.
    """

    def test_geojson_file_writer_survives_narrow_locale(self, tmp_path, narrow_open):
        out = str(tmp_path / "out.geojson")
        with GeoJSONWriter(out) as writer:
            writer.write_feature(
                '{"type":"Point","coordinates":[0,0]}',
                {"name": _NON_ASCII_CHARS},
            )
        content = json.loads((tmp_path / "out.geojson").read_text(encoding="utf-8"))
        assert content["features"][0]["properties"]["name"] == _NON_ASCII_CHARS

    def test_geojsonseq_file_writer_survives_narrow_locale(self, tmp_path, narrow_open):
        out = str(tmp_path / "out.geojsonseq")
        with GeoJSONSeqWriter(out) as writer:
            writer.write_feature(
                '{"type":"Point","coordinates":[0,0]}',
                {"name": _NON_ASCII_CHARS},
            )
        content = json.loads((tmp_path / "out.geojsonseq").read_text(encoding="utf-8"))
        assert content["properties"]["name"] == _NON_ASCII_CHARS

    def test_without_fix_narrow_locale_would_fail(self, tmp_path, narrow_open):
        """Confirm narrow_open fixture actually breaks un-guarded open() calls.

        This test documents the failure mode: if writers ever drop encoding=,
        this assertion shows what would blow up.
        """
        out = str(tmp_path / "broken.txt")
        with pytest.raises(UnicodeEncodeError):
            with builtins.open(out, "w") as f:  # no encoding= → ascii → boom
                f.write(_NON_ASCII_CHARS)
