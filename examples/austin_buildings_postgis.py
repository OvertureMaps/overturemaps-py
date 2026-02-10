"""Example: initialize and incrementally update Austin buildings using PostGIS.

Requires a PostgreSQL database with the PostGIS extension.

Set DATABASE_URL environment variable before running, e.g.:
  export DATABASE_URL="postgresql://user:pass@localhost/overture_demo"
"""

from __future__ import annotations

import os

from overturemaps.backends.postgis import PostGISBackend
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
TABLE = "austin_buildings"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/overture_demo")

# State file is automatically determined from database URL
STATE_FILE = get_state_file_for_backend(Backend.postgis, None, DATABASE_URL)


def main() -> None:
    latest = get_latest_release()
    print(f"Latest release: {latest}")

    state = load_state(STATE_FILE)
    backend = PostGISBackend(DATABASE_URL, TABLE)

    if state is None:
        print("No existing state found. Performing full initialization…")
        features = geodataframe(
            "building",
            bbox=(BBOX.xmin, BBOX.ymin, BBOX.xmax, BBOX.ymax),
            release=latest,
        )
        backend.upsert(features)
        print(f"Initialized with {backend.count()} features.")
    elif state.last_release == latest:
        print(f"Already up to date at release {latest}.")
        return
    else:
        print(f"Updating {state.last_release} → {latest} …")
        ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
            latest, THEME, TYPE, BBOX
        )
        ids_to_fetch = ids_to_add | ids_to_modify

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
        backend=Backend.postgis,
        output=DATABASE_URL,
    )
    save_state(new_state, STATE_FILE)
    print(f"State saved to: {STATE_FILE}")


if __name__ == "__main__":
    main()
