"""Pipeline state management for tracking downloads and updates."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .models import PipelineState


def get_state_path(output_path: str) -> Path:
    """Get the state file path for a given output file.

    Args:
        output_path: Path to the output file.

    Returns:
        Path to the state file (output_path + ".state").
    """
    return Path(f"{output_path}.state")


def load_state(state_path: Path | str) -> Optional[PipelineState]:
    """Load pipeline state from a JSON file.

    Args:
        state_path: Path to the state file.

    Returns:
        PipelineState object if file exists, None otherwise.
    """
    state_path = Path(state_path)
    if not state_path.exists():
        return None

    try:
        with open(state_path, "r") as f:
            data = json.load(f)
        return PipelineState.from_dict(data)
    except (json.JSONDecodeError, KeyError, FileNotFoundError):
        return None


def save_state(state: PipelineState, state_path: Path | str) -> None:
    """Save pipeline state to a JSON file.

    Args:
        state: PipelineState object to save.
        state_path: Path to the state file.
    """
    state_path = Path(state_path)
    # Create parent directory if it doesn't exist
    state_path.parent.mkdir(parents=True, exist_ok=True)

    with open(state_path, "w") as f:
        json.dump(state.as_dict(), f, indent=2)
