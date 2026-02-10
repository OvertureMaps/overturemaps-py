"""Data models for the Overture update toolkit."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ChangeType(StrEnum):
    """Type of change recorded in the GERS changelog."""

    added = "added"
    modified = "modified"
    deprecated = "deprecated"


class Backend(StrEnum):
    """Storage backend for local Overture data."""

    geojson = "geojson"
    geojsonseq = "geojsonseq"
    geoparquet = "geoparquet"
    postgis = "postgis"


@dataclass
class BBox:
    """Axis-aligned bounding box (WGS84 lon/lat)."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float

    def as_tuple(self) -> tuple[float, float, float, float]:
        """Return (xmin, ymin, xmax, ymax) tuple."""
        return (self.xmin, self.ymin, self.xmax, self.ymax)

    def as_wkt(self) -> str:
        """Return WKT POLYGON representation for spatial queries."""
        return (
            f"POLYGON(({self.xmin} {self.ymin}, {self.xmax} {self.ymin}, "
            f"{self.xmax} {self.ymax}, {self.xmin} {self.ymax}, "
            f"{self.xmin} {self.ymin}))"
        )


@dataclass
class ChangeRecord:
    """A single record from the Overture GERS changelog."""

    id: str
    change_type: ChangeType
    successor_ids: list[str] = field(default_factory=list)
    bbox: BBox | None = None


@dataclass
class PipelineState:
    """Persistent state tracking for an update pipeline."""

    last_release: str
    last_run: str  # ISO 8601 datetime string
    theme: str
    type: str
    bbox: BBox
    backend: Backend
    output: str | None = None  # file path or db_url depending on backend
