"""Tests for download CLI command."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from overturemaps.cli import (
    BboxParamType,
    _bbox_area_sq_deg,
    cli,
)


class _DummyWriter:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        return False


class _DummyReader:
    schema = object()


def test_download_saves_absolute_output_path(monkeypatch):
    """`download` stores absolute output path in saved state."""

    captured = {}

    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2024-11-13.0")
    monkeypatch.setattr("overturemaps.cli.count_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        "overturemaps.cli.record_batch_reader", lambda *args, **kwargs: _DummyReader()
    )
    monkeypatch.setattr(
        "overturemaps.cli.get_writer", lambda *args, **kwargs: _DummyWriter()
    )
    monkeypatch.setattr("overturemaps.cli.copy", lambda *args, **kwargs: None)

    def _fake_save_state(state, state_path):
        captured["state"] = state
        captured["state_path"] = state_path

    monkeypatch.setattr("overturemaps.cli.save_state", _fake_save_state)

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli,
            [
                "download",
                "-f",
                "geojson",
                "-t",
                "building",
                "-o",
                "relative-output.geojson",
            ],
        )

        assert result.exit_code == 0
        assert "state" in captured
        assert captured["state"].bbox is None
        assert captured["state"].output == str(
            Path("relative-output.geojson").resolve()
        )


# --- BboxParamType validation tests ---


class TestBboxParamType:
    """Tests for BboxParamType.convert() error messages."""

    def _convert(self, value):
        """Helper: invoke the param type's convert method."""
        param_type = BboxParamType()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["download", "-f", "geojson", "-t", "building", "--bbox", value],
        )
        return result

    def test_valid_bbox(self):
        param_type = BboxParamType()
        result = param_type.convert("-71.10,42.34,-71.05,42.36", None, None)
        assert result == [-71.10, 42.34, -71.05, 42.36]

    def test_wrong_number_of_values(self):
        result = self._convert("1,2,3")
        assert result.exit_code != 0
        assert "exactly 4 values" in result.output

    def test_non_numeric_values(self):
        result = self._convert("a,b,c,d")
        assert result.exit_code != 0
        assert "must be numbers" in result.output

    def test_longitude_out_of_range(self):
        result = self._convert("-200,42,-71,43")
        assert result.exit_code != 0
        assert "Longitude" in result.output
        assert "-180" in result.output

    def test_latitude_out_of_range(self):
        result = self._convert("-71,-100,-70,42")
        assert result.exit_code != 0
        assert "Latitude" in result.output
        assert "-90" in result.output

    def test_swapped_xmin_xmax(self):
        result = self._convert("10,42,-10,43")
        assert result.exit_code != 0
        assert "xmin" in result.output
        assert "xmax" in result.output

    def test_swapped_ymin_ymax(self):
        result = self._convert("-71,43,-70,42")
        assert result.exit_code != 0
        assert "ymin" in result.output
        assert "ymax" in result.output

    def test_example_shown_in_error(self):
        """Error messages should include a usage example."""
        result = self._convert("1,2,3")
        assert "Example" in result.output or "--bbox" in result.output


# --- Area helper ---


def test_bbox_area_sq_deg():
    assert _bbox_area_sq_deg(0, 0, 10, 10) == 100.0
    assert _bbox_area_sq_deg(-180, -90, 180, 90) == pytest.approx(64800.0)


# --- Large bbox warning in download command ---


def test_download_warns_on_large_bbox(monkeypatch):
    """download should warn when bbox is very large."""
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2024-11-13.0")
    monkeypatch.setattr("overturemaps.cli.count_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        "overturemaps.cli.record_batch_reader", lambda *args, **kwargs: _DummyReader()
    )
    monkeypatch.setattr(
        "overturemaps.cli.get_writer", lambda *args, **kwargs: _DummyWriter()
    )
    monkeypatch.setattr("overturemaps.cli.copy", lambda *args, **kwargs: None)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geojson",
            "-t",
            "building",
            "--bbox",
            "-180,-90,180,90",
        ],
    )
    assert result.exit_code == 0
    assert "Warning" in result.output
    assert "1.2 TB" in result.output
    assert "400 GB" in result.output


def test_download_warns_on_no_bbox(monkeypatch):
    """download should warn when no bbox is provided."""
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2024-11-13.0")
    monkeypatch.setattr("overturemaps.cli.count_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        "overturemaps.cli.record_batch_reader", lambda *args, **kwargs: _DummyReader()
    )
    monkeypatch.setattr(
        "overturemaps.cli.get_writer", lambda *args, **kwargs: _DummyWriter()
    )
    monkeypatch.setattr("overturemaps.cli.copy", lambda *args, **kwargs: None)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geojson",
            "-t",
            "building",
        ],
    )
    assert result.exit_code == 0
    assert "Warning" in result.output
    assert "No bounding box" in result.output
    assert "1.2 TB" in result.output


def test_download_no_warning_on_small_bbox(monkeypatch):
    """download should not warn when bbox is small."""
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2024-11-13.0")
    monkeypatch.setattr("overturemaps.cli.count_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        "overturemaps.cli.record_batch_reader", lambda *args, **kwargs: _DummyReader()
    )
    monkeypatch.setattr(
        "overturemaps.cli.get_writer", lambda *args, **kwargs: _DummyWriter()
    )
    monkeypatch.setattr("overturemaps.cli.copy", lambda *args, **kwargs: None)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geojson",
            "-t",
            "building",
            "--bbox",
            "-71.10,42.34,-71.05,42.36",
        ],
    )
    assert result.exit_code == 0
    assert "Warning" not in result.output
