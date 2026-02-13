"""Persistent state management for the Overture update pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from .models import Backend, BBox, PipelineState


def get_state_file_for_backend(
    backend: Backend, output: str | None, db_url: str | None
) -> Path | str:
    """Automatically determine the state location based on backend and output.

    For file-based backends (geojson, geojsonseq, geoparquet), returns {output}.state as a Path.
    For postgis backend, returns the db_url (state is stored in the database).

    Args:
        backend: The storage backend type.
        output: Output file path for file-based backends.
        db_url: Database URL for postgis backend.

    Returns:
        Path to the state file for file-based backends, or db_url for PostGIS.

    Raises:
        ValueError: If neither output nor db_url is provided.
    """
    if backend == Backend.postgis:
        # For PostGIS, state is stored in the database itself
        if db_url:
            return db_url
        raise ValueError("db_url is required for postgis backend")
    else:
        # For file-based backends, use {output}.state
        if output:
            return Path(str(output) + ".state")
        raise ValueError("output is required for file-based backends")


def load_state(state_location: Path | str) -> PipelineState | None:
    """Load pipeline state from a file or database.

    Args:
        state_location: Path to JSON file for file-based backends, or db_url for PostGIS.

    Returns:
        PipelineState if the state exists and is valid, None otherwise.
    """
    if isinstance(state_location, str):
        # Database-backed state (PostGIS)
        return _load_state_from_db(state_location)
    else:
        # File-based state
        return _load_state_from_file(state_location)


def _load_state_from_file(state_file: Path) -> PipelineState | None:
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


def _load_state_from_db(db_url: str) -> PipelineState | None:
    """Load pipeline state from PostGIS database.

    Args:
        db_url: Database connection URL.

    Returns:
        PipelineState if state exists in database, None otherwise.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            # Check if state table exists
            result = conn.execute(
                text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'overture_state');"
                )
            )
            if not result.scalar():
                return None

            # Load state from table
            result = conn.execute(
                text("SELECT state_json FROM public.overture_state LIMIT 1;")
            )
            row = result.fetchone()
            if not row:
                return None

            data = json.loads(row[0])
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
    except Exception as exc:
        raise ValueError(f"Error loading state from database: {exc}") from exc
    finally:
        engine.dispose()


def save_state(state: PipelineState, state_location: Path | str) -> None:
    """Persist pipeline state to a file or database.

    Args:
        state: PipelineState to persist.
        state_location: Path to JSON file for file-based backends, or db_url for PostGIS.
    """
    if isinstance(state_location, str):
        # Database-backed state (PostGIS)
        _save_state_to_db(state, state_location)
    else:
        # File-based state
        _save_state_to_file(state, state_location)


def _save_state_to_file(state: PipelineState, state_file: Path) -> None:
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


def _save_state_to_db(state: PipelineState, db_url: str) -> None:
    """Persist pipeline state to PostGIS database.

    Creates the state table if it doesn't exist and upserts the state.

    Args:
        state: PipelineState to persist.
        db_url: Database connection URL.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url)

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

    try:
        with engine.begin() as conn:
            # Create state table if it doesn't exist
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS public.overture_state (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        state_json TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT single_row CHECK (id = 1)
                    );
                    """
                )
            )

            # Upsert state (only one row allowed due to constraint)
            conn.execute(
                text(
                    """
                    INSERT INTO public.overture_state (id, state_json, updated_at)
                    VALUES (1, :state_json, CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET
                        state_json = EXCLUDED.state_json,
                        updated_at = EXCLUDED.updated_at;
                    """
                ),
                {"state_json": json.dumps(data)},
            )
    finally:
        engine.dispose()
