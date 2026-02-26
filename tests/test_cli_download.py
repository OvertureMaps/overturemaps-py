"""Tests for download CLI command."""

from pathlib import Path

from click.testing import CliRunner

from overturemaps.cli import cli


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
