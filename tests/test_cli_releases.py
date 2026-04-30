"""Tests for releases CLI commands."""

from click.testing import CliRunner

from overturemaps.cli import cli
from overturemaps.models import Backend, PipelineState


def test_releases_list(monkeypatch):
    """`releases list` prints all releases."""
    monkeypatch.setattr(
        "overturemaps.cli.list_releases",
        lambda: ["2025-01-01.0", "2024-12-01.0", "2024-11-01.0"],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "list"])
    assert result.exit_code == 0
    assert "2025-01-01.0" in result.output
    assert "2024-12-01.0" in result.output
    assert "2024-11-01.0" in result.output


def test_releases_list_empty(monkeypatch):
    """`releases list` handles empty list without crashing."""
    monkeypatch.setattr("overturemaps.cli.list_releases", lambda: [])
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "list"])
    assert result.exit_code == 0


def test_releases_latest(monkeypatch):
    """`releases latest` prints the latest release."""
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2025-01-01.0")
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "latest"])
    assert result.exit_code == 0
    assert "2025-01-01.0" in result.output


def test_releases_check_up_to_date(monkeypatch, tmp_path):
    """`releases check` exits 0 when file matches latest release."""
    output_file = tmp_path / "out.geojson"
    output_file.touch()
    mock_state = PipelineState(
        last_release="2025-01-01.0",
        last_run="2025-01-01T00:00:00Z",
        theme="buildings",
        type="building",
        bbox=None,
        backend=Backend.geojson,
        output=str(output_file),
    )
    monkeypatch.setattr("overturemaps.cli.load_state", lambda path: mock_state)
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2025-01-01.0")
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 0
    assert "Up to date" in result.output


def test_releases_check_update_available(monkeypatch, tmp_path):
    """`releases check` exits 1 when a newer release exists."""
    output_file = tmp_path / "out.geojson"
    output_file.touch()
    mock_state = PipelineState(
        last_release="2024-11-01.0",
        last_run="2024-11-01T00:00:00Z",
        theme="buildings",
        type="building",
        bbox=None,
        backend=Backend.geojson,
        output=str(output_file),
    )
    monkeypatch.setattr("overturemaps.cli.load_state", lambda path: mock_state)
    monkeypatch.setattr("overturemaps.cli.get_latest_release", lambda: "2025-01-01.0")
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 1
    assert "Update available" in result.output


def test_releases_check_no_state_file(monkeypatch, tmp_path):
    """`releases check` exits 1 when no state file found."""
    output_file = tmp_path / "out.geojson"
    output_file.touch()
    monkeypatch.setattr("overturemaps.cli.load_state", lambda path: None)
    runner = CliRunner()
    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 1


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
