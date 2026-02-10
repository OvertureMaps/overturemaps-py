"""Shared pytest fixtures for the overture-update-toolkit test suite."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Point


@pytest.fixture()
def temp_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for test output files."""
    return tmp_path


@pytest.fixture()
def sample_bbox_dict() -> dict:
    """Return a sample bounding box dict (Austin, TX area)."""
    return {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4}


@pytest.fixture()
def sample_features_gdf() -> gpd.GeoDataFrame:
    """Return a small sample GeoDataFrame mimicking Overture features."""
    data = {
        "id": ["feat_001", "feat_002", "feat_003"],
        "theme": ["buildings", "buildings", "buildings"],
        "geometry": [
            Point(-97.74, 30.27),
            Point(-97.73, 30.28),
            Point(-97.72, 30.29),
        ],
    }
    return gpd.GeoDataFrame(data, crs="EPSG:4326")


@pytest.fixture()
def sample_changelog_records() -> list[dict]:
    """Return raw changelog record dicts for testing parsing logic."""
    return [
        {
            "id": "feat_001",
            "change_type": "added",
            "successor_ids": [],
            "bbox": {"xmin": -97.75, "ymin": 30.25, "xmax": -97.74, "ymax": 30.26},
        },
        {
            "id": "feat_002",
            "change_type": "modified",
            "successor_ids": [],
            "bbox": {"xmin": -97.74, "ymin": 30.26, "xmax": -97.73, "ymax": 30.27},
        },
        {
            "id": "feat_003",
            "change_type": "deprecated",
            "successor_ids": ["feat_004", "feat_005"],
            "bbox": {"xmin": -97.73, "ymin": 30.27, "xmax": -97.72, "ymax": 30.28},
        },
        {
            "id": "feat_006",
            "change_type": "deprecated",
            "successor_ids": [],
            "bbox": {"xmin": -98.0, "ymin": 31.0, "xmax": -97.9, "ymax": 31.1},
        },
    ]


@pytest.fixture()
def mock_s3_releases() -> list[str]:
    """Return a list of fake release IDs."""
    return ["2024-11-13.0", "2024-10-23.0", "2024-09-18.0"]


@pytest.fixture()
def sample_state_json(temp_dir: Path) -> Path:
    """Write a sample state.json and return its path."""
    state_data = {
        "last_release": "2024-10-23.0",
        "last_run": "2024-10-24T12:00:00+00:00",
        "theme": "buildings",
        "type": "building",
        "bbox": {"xmin": -97.8, "ymin": 30.2, "xmax": -97.6, "ymax": 30.4},
        "backend": "geoparquet",
        "output": "/tmp/austin_buildings.parquet",
    }
    path = temp_dir / "state.json"
    path.write_text(json.dumps(state_data))
    return path
