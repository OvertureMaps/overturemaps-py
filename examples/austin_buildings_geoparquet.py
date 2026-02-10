"""Example: initialize and incrementally update Austin buildings using GeoParquet.

Run this script once to download all buildings for central Austin, then run it
again (after a new Overture release) to apply the incremental update.
"""

from __future__ import annotations

from pathlib import Path

from overturemaps.backends.geoparquet import GeoParquetBackend
from overturemaps.changelog import query_changelog_ids
from overturemaps.core import geodataframe
from overturemaps.fetch import fetch_features
from overturemaps.models import BBox, Backend
from overturemaps.releases import get_latest_release
from overturemaps.state import load_state, save_state, get_state_file_for_backend

# Configuration
BBOX = BBox(xmin=-97.8, ymin=30.2, xmax=-97.6, ymax=30.4)
THEME = "buildings"
TYPE = "building"
OUTPUT_PATH = Path.home() / "data" / "austin_buildings.parquet"

# State file is automatically determined from output path
STATE_FILE = get_state_file_for_backend(Backend.geoparquet, str(OUTPUT_PATH), None)


def main() -> None:
    latest = get_latest_release()
    print(f"Latest release: {latest}")

    state = load_state(STATE_FILE)

    if state is None:
        # First run — full download
        print("No existing state found. Performing full initialization…")
        features = geodataframe(
            "building",
            bbox=(BBOX.xmin, BBOX.ymin, BBOX.xmax, BBOX.ymax),
            release=latest,
        )
        backend = GeoParquetBackend(OUTPUT_PATH)
        backend.upsert(features)
        print(f"Initialized with {backend.count()} features.")
    elif state.last_release == latest:
        print(f"Already up to date at release {latest}.")
        return
    else:
        # Incremental update
        print(f"Updating {state.last_release} → {latest} …")
        ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
            latest, THEME, TYPE, BBOX
        )
        ids_to_fetch = ids_to_add | ids_to_modify

        backend = GeoParquetBackend(OUTPUT_PATH)
        if ids_to_fetch:
            features = fetch_features(latest, THEME, TYPE, ids_to_fetch, BBOX)
            backend.upsert(features)
        if ids_to_delete:
            backend.delete(ids_to_delete)
        print(
            f"Applied: +{len(ids_to_add)} ~{len(ids_to_modify)} -{len(ids_to_delete)}. "
            f"Total: {backend.count()} features."
        )

    # Persist state
    from datetime import datetime, timezone
    from overturemaps.models import Backend, PipelineState

    new_state = PipelineState(
        last_release=latest,
        last_run=datetime.now(timezone.utc).isoformat(),
        theme=THEME,
        type=TYPE,
        bbox=BBOX,
        backend=Backend.geoparquet,
        output=str(OUTPUT_PATH),
    )
    save_state(new_state, STATE_FILE)
    print(f"State saved to: {STATE_FILE}")


if __name__ == "__main__":
    main()
