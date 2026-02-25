"""Tests for releases CLI commands."""

from click.testing import CliRunner

from overturemaps.cli import cli


def test_releases_exists_true(monkeypatch):
    """`releases exists` prints true when release exists."""

    monkeypatch.setattr("overturemaps.cli.release_exists", lambda release: True)
    runner = CliRunner()

    result = runner.invoke(cli, ["releases", "exists", "2025-12-17.0"])

    assert result.exit_code == 0
    assert result.output.strip() == "true"


def test_releases_exists_false(monkeypatch):
    """`releases exists` fails when release does not exist."""

    monkeypatch.setattr("overturemaps.cli.release_exists", lambda release: False)
    runner = CliRunner()

    result = runner.invoke(cli, ["releases", "exists", "1900-01-01.0"])

    assert result.exit_code == 1
    assert "Release '1900-01-01.0' not found" in result.output
