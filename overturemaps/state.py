"""Persistent state management for the Overture update pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from .models import Backend, BBox, PipelineState

DEFAULT_STATE_DIR = Path.home() / ".overture"
DEFAULT_STATE_FILE = DEFAULT_STATE_DIR / "state.json"


def get_state_file_for_backend(
    backend: Backend, output: str | None, db_url: str | None
) -> Path:
    """Automatically determine the state file location based on backend and output.

    For file-based backends (geojson, geojsonseq, geoparquet), returns {output}.state
    For postgis backend, returns a default location based on database URL hash.

    Args:
        backend: The storage backend type.
        output: Output file path for file-based backends.
        db_url: Database URL for postgis backend.

    Returns:
        Path to the state file.

    Raises:
        ValueError: If neither output nor db_url is provided.
    """
    if backend == Backend.postgis:
        # For PostGIS, create a unique state file based on db_url
        if db_url:
            import hashlib

            db_hash = hashlib.md5(db_url.encode()).hexdigest()[:8]
            state_dir = DEFAULT_STATE_DIR / "postgis"
            state_dir.mkdir(parents=True, exist_ok=True)
            return state_dir / f"state_{db_hash}.json"
        raise ValueError("db_url is required for postgis backend")
    else:
        # For file-based backends, use {output}.state
        if output:
            return Path(str(output) + ".state")
        raise ValueError("output is required for file-based backends")


def load_state(state_file: Path = DEFAULT_STATE_FILE) -> PipelineState | None:
    """Load pipeline state from a JSON file.

    Args:
        state_file: Path to the JSON state file.

    Returns:
        PipelineState if the file exists and is valid, None otherwise.
    """
    if not state_file.exists():
        return None

    try:
        data = json.loads(state_file.read_text())
        bbox_data = data["bbox"]
        return PipelineState(
            last_release=data["last_release"],
            last_run=data["last_run"],
            theme=data["theme"],
            type=data["type"],
            bbox=BBox(
                xmin=bbox_data["xmin"],
                ymin=bbox_data["ymin"],
                xmax=bbox_data["xmax"],
                ymax=bbox_data["ymax"],
            ),
            backend=Backend(data["backend"]),
            output=data.get("output"),
        )
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid state file at {state_file}: {exc}") from exc


def save_state(state: PipelineState, state_file: Path = DEFAULT_STATE_FILE) -> None:
    """Persist pipeline state to a JSON file.

    Creates the parent directory if it does not exist.

    Args:
        state: PipelineState to persist.
        state_file: Path where the JSON file should be written.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "last_release": state.last_release,
        "last_run": state.last_run,
        "theme": state.theme,
        "type": state.type,
        "bbox": {
            "xmin": state.bbox.xmin,
            "ymin": state.bbox.ymin,
            "xmax": state.bbox.xmax,
            "ymax": state.bbox.ymax,
        },
        "backend": state.backend.value,
        "output": state.output,
    }
    state_file.write_text(json.dumps(data, indent=2))
