"""Tests for the overture CLI using Click's test runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest
from click.testing import CliRunner

from overturemaps.cli import cli
from overturemaps.models import Backend, BBox, ChangeRecord, ChangeType, PipelineState

runner = CliRunner()


# ---------------------------------------------------------------------------
# Releases Commands Tests
# ---------------------------------------------------------------------------


@patch("overturemaps.releases.get_latest_release")
def test_releases_latest(mock_latest):
    """releases latest prints the most recent release."""
    mock_latest.return_value = "2024-11-13.0"

    result = runner.invoke(cli, ["releases", "latest"])
    assert result.exit_code == 0
    assert result.output.strip() == "2024-11-13.0"
    mock_latest.assert_called_once()


@patch("overturemaps.releases.list_releases")
def test_releases_list(mock_list):
    """releases list prints available releases."""
    mock_list.return_value = ["2024-11-13.0", "2024-10-23.0", "2024-09-18.0"]

    result = runner.invoke(cli, ["releases", "list"])
    assert result.exit_code == 0

    lines = result.output.strip().split("\n")
    assert len(lines) == 3
    assert "2024-11-13.0" in lines[0]
    assert "2024-10-23.0" in lines[1]
    assert "2024-09-18.0" in lines[2]
    mock_list.assert_called_once()


def test_releases_check_requires_output():
    """releases check requires either --output or --db-url."""
    result = runner.invoke(cli, ["releases", "check"])
    assert result.exit_code != 0
    assert "Must specify either --output or --db-url" in result.output


@patch("overturemaps.releases.get_latest_release")
def test_releases_check_up_to_date(mock_latest, temp_dir):
    """releases check reports up to date when state matches latest release."""
    output_file = temp_dir / "test.parquet"
    state_file = temp_dir / "test.parquet.state"

    # Mock latest release to match state
    mock_latest.return_value = "2024-01-01.0"

    # Create a state file at the latest release
    state_data = {
        "last_release": "2024-01-01.0",
        "last_run": "2024-01-02T00:00:00Z",
        "theme": "buildings",
        "type": "building",
        "bbox": {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4},
        "backend": "geoparquet",
        "output": str(output_file),
    }
    state_file.write_text(json.dumps(state_data))

    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 0
    assert "Up to date (release 2024-01-01.0)" in result.output


@patch("overturemaps.releases.get_latest_release")
def test_releases_check_update_available(mock_latest, temp_dir):
    """releases check reports update available when newer release exists."""
    output_file = temp_dir / "test.parquet"
    state_file = temp_dir / "test.parquet.state"

    # Mock latest release to be newer than state
    mock_latest.return_value = "2024-02-01.0"

    # Create a state file at an older release
    state_data = {
        "last_release": "2024-01-01.0",
        "last_run": "2024-01-02T00:00:00Z",
        "theme": "buildings",
        "type": "building",
        "bbox": {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4},
        "backend": "geoparquet",
        "output": str(output_file),
    }
    state_file.write_text(json.dumps(state_data))

    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 1
    assert "Update available: 2024-01-01.0 → 2024-02-01.0" in result.output


def test_releases_check_no_state_file(temp_dir):
    """releases check with missing state file reports error."""
    output_file = temp_dir / "nonexistent.parquet"
    result = runner.invoke(cli, ["releases", "check", "-o", str(output_file)])
    assert result.exit_code == 2
    assert "No state found" in result.output


# ---------------------------------------------------------------------------
# Update Status Tests
# ---------------------------------------------------------------------------


def test_update_status_requires_output():
    """update status requires either --output or --db-url."""
    result = runner.invoke(cli, ["update", "status"])
    assert result.exit_code != 0
    assert "Must specify either --output or --db-url" in result.output


def test_update_status_with_state_file(temp_dir):
    """update status displays pipeline state from state file."""
    output_file = temp_dir / "data.parquet"
    state_file = temp_dir / "data.parquet.state"

    state_data = {
        "last_release": "2024-10-23.0",
        "last_run": "2024-10-24T12:00:00+00:00",
        "theme": "buildings",
        "type": "building",
        "bbox": {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4},
        "backend": "geoparquet",
        "output": str(output_file),
    }
    state_file.write_text(json.dumps(state_data))

    result = runner.invoke(cli, ["update", "status", "-o", str(output_file)])
    assert result.exit_code == 0
    assert "2024-10-23.0" in result.output
    assert "buildings" in result.output


def test_update_status_no_state(temp_dir):
    """update status exits 1 when no state file exists."""
    output_file = temp_dir / "missing.parquet"
    result = runner.invoke(cli, ["update", "status", "-o", str(output_file)])
    assert result.exit_code == 1
    assert "No state found" in result.output


# ---------------------------------------------------------------------------
# Download Command Tests (Streaming)
# ---------------------------------------------------------------------------


@patch("overturemaps.cli.record_batch_reader")
def test_download_streaming_to_stdout(mock_reader):
    """download without output streams to stdout."""
    # Mock reader that returns empty batches
    mock_reader.return_value = pa.RecordBatchReader.from_batches(
        pa.schema([("id", pa.string()), ("geometry", pa.binary())]), []
    )

    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geojsonseq",
            "-t",
            "building",
        ],
    )

    # Should succeed even with no bbox (will try to download everything)
    assert result.exit_code == 0
    assert mock_reader.called


@patch("overturemaps.cli.record_batch_reader")
@patch("overturemaps.backends.geoparquet.GeoParquetBackend.write_from_reader")
def test_download_to_file_creates_state(mock_write, mock_reader, temp_dir):
    """download with --output creates state file automatically."""
    output_file = temp_dir / "buildings.parquet"
    state_file = temp_dir / "buildings.parquet.state"

    # Mock reader
    schema = pa.schema([("id", pa.string()), ("geometry", pa.binary())])
    mock_reader.return_value = pa.RecordBatchReader.from_batches(schema, [])
    mock_write.return_value = 100  # 100 features written

    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geoparquet",
            "-t",
            "building",
            "--bbox",
            "-97.8,30.2,-97.6,30.4",
            "-o",
            str(output_file),
        ],
    )

    assert result.exit_code == 0
    assert "Downloaded 100 features" in result.output
    assert "State saved to" in result.output

    # State file should be created
    assert state_file.exists()
    state_data = json.loads(state_file.read_text())
    assert state_data["backend"] == "geoparquet"
    assert state_data["type"] == "building"


@patch("overturemaps.cli.record_batch_reader")
@patch("overturemaps.backends.geojsonseq.GeoJSONSeqBackend.write_from_reader")
def test_download_geojsonseq_streaming(mock_write, mock_reader, temp_dir):
    """download to geojsonseq uses streaming backend."""
    output_file = temp_dir / "buildings.geojsonl"

    schema = pa.schema([("id", pa.string()), ("geometry", pa.binary())])
    mock_reader.return_value = pa.RecordBatchReader.from_batches(schema, [])
    mock_write.return_value = 50

    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geojsonseq",
            "-t",
            "building",
            "--bbox",
            "-97.8,30.2,-97.6,30.4",
            "-o",
            str(output_file),
        ],
    )

    assert result.exit_code == 0
    assert mock_write.called
    # Should have called write_from_reader on backend
    assert mock_write.call_count == 1


# ---------------------------------------------------------------------------
# Update Run Tests
# ---------------------------------------------------------------------------


@patch("overturemaps.releases.get_latest_release")
@patch("overturemaps.changelog.query_changelog_ids")
def test_update_run_dry_run(mock_query, mock_latest, temp_dir):
    """update run --dry-run shows changes without applying."""
    output_file = temp_dir / "buildings.parquet"
    state_file = temp_dir / "buildings.parquet.state"

    # Create initial state
    state_data = {
        "last_release": "2024-10-23.0",
        "last_run": "2024-10-24T00:00:00Z",
        "theme": "buildings",
        "type": "building",
        "bbox": {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4},
        "backend": "geoparquet",
        "output": str(output_file),
    }
    state_file.write_text(json.dumps(state_data))

    # Mock latest release and changelog
    mock_latest.return_value = "2024-11-13.0"
    mock_query.return_value = ({"id1"}, {"id2"}, {"id3"})  # added, modified, deleted

    result = runner.invoke(
        cli,
        [
            "update",
            "run",
            "--theme",
            "buildings",
            "--type",
            "building",
            "--bbox",
            "-97.8,30.2,-97.6,30.4",
            "-f",
            "geoparquet",
            "-o",
            str(output_file),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "Dry run" in result.output
    assert "Changes:" in result.output


@patch("overturemaps.releases.get_latest_release")
@patch("overturemaps.state.load_state")
def test_update_run_already_up_to_date(mock_load, mock_latest, temp_dir):
    """update run exits early when already at target release."""
    output_file = temp_dir / "buildings.parquet"

    # Mock latest release
    mock_latest.return_value = "2024-11-13.0"

    # Create state that's already at latest
    mock_load.return_value = PipelineState(
        last_release="2024-11-13.0",
        last_run="2024-11-14T00:00:00Z",
        theme="buildings",
        type="building",
        bbox=BBox(-97.8, 30.2, -97.6, 30.4),
        backend=Backend.geoparquet,
        output=str(output_file),
    )

    result = runner.invoke(
        cli,
        [
            "update",
            "run",
            "--theme",
            "buildings",
            "--type",
            "building",
            "--bbox",
            "-97.8,30.2,-97.6,30.4",
            "-f",
            "geoparquet",
            "-o",
            str(output_file),
        ],
    )

    assert result.exit_code == 0
    assert "up to date" in result.output.lower()


# ---------------------------------------------------------------------------
# Changelog Query Tests
# ---------------------------------------------------------------------------


@patch("overturemaps.changelog.summarize_changelog")
def test_changelog_query_summary(mock_summarize):
    """changelog summary shows counts by change type."""

    # Return nested dict matching new summarize_changelog format: {theme: {type: {change_type: count}}}
    mock_summarize.return_value = {
        "buildings": {
            "building": {
                ChangeType.added: 10,
                ChangeType.modified: 5,
                ChangeType.deprecated: 3,
            }
        }
    }

    result = runner.invoke(
        cli,
        [
            "changelog",
            "summary",
            "--release",
            "2024-11-13.0",
            "--theme",
            "buildings",
            "--type",
            "building",
        ],
    )

    assert result.exit_code == 0
    assert "added" in result.output.lower() or "modified" in result.output.lower()


# ---------------------------------------------------------------------------
# Backend Resolution Tests
# ---------------------------------------------------------------------------


def test_download_postgis_requires_db_url(temp_dir):
    """download with postgis format requires --db-url."""
    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "postgis",
            "-t",
            "building",
            "--bbox",
            "-97.8,30.2,-97.6,30.4",
        ],
    )

    # Should fail because no db-url provided
    assert result.exit_code != 0


def test_download_geoparquet_requires_output():
    """download with geoparquet to backend requires --output."""
    # This should work for stdout mode but fail for backend mode
    # Backend mode is triggered by presence of output
    result = runner.invoke(
        cli,
        [
            "download",
            "-f",
            "geoparquet",
            "-t",
            "building",
        ],
    )

    # Should require output for geoparquet in the error
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# State File Location Tests
# ---------------------------------------------------------------------------


def test_state_file_location_geoparquet(temp_dir):
    """State file for geoparquet is {output}.state."""
    from overturemaps.state import get_state_file_for_backend
    from overturemaps.models import Backend

    output = temp_dir / "data.parquet"
    state_path = get_state_file_for_backend(Backend.geoparquet, str(output), None)

    assert state_path == Path(str(output) + ".state")


def test_state_file_location_postgis():
    """State file for postgis returns the db_url (state stored in database)."""
    from overturemaps.state import get_state_file_for_backend
    from overturemaps.models import Backend

    db_url = "postgresql://user:pass@localhost/db"
    state_location = get_state_file_for_backend(Backend.postgis, None, db_url)

    # Should return the db_url string (state is stored in the database)
    assert state_location == db_url
    assert isinstance(state_location, str)


def test_state_file_location_requires_params():
    """get_state_file_for_backend raises error without required params."""
    from overturemaps.state import get_state_file_for_backend
    from overturemaps.models import Backend

    # Should raise error for geoparquet without output
    with pytest.raises(ValueError, match="output is required"):
        get_state_file_for_backend(Backend.geoparquet, None, None)

    # Should raise error for postgis without db_url
    with pytest.raises(ValueError, match="db_url is required"):
        get_state_file_for_backend(Backend.postgis, None, None)
