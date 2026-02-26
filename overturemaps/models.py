"""Data models for the Overture toolkit."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# Compatibility shim for Python 3.10 (StrEnum was added in 3.11)
try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, Enum):
        """String enumeration for Python < 3.11 compatibility."""

        def __str__(self) -> str:
            return str(self.value)

        @staticmethod
        def _generate_next_value_(name, start, count, last_values):
            return name.lower()


class Backend(StrEnum):
    """Storage backend for local Overture data."""

    geojson = "geojson"
    geojsonseq = "geojsonseq"
    geoparquet = "geoparquet"


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

    def as_dict(self) -> dict[str, float]:
        """Return bbox as a dictionary."""
        return {
            "xmin": self.xmin,
            "ymin": self.ymin,
            "xmax": self.xmax,
            "ymax": self.ymax,
        }

    @classmethod
    def from_dict(cls, data: dict[str, float]) -> BBox:
        """Create BBox from a dictionary."""
        return cls(
            xmin=data["xmin"],
            ymin=data["ymin"],
            xmax=data["xmax"],
            ymax=data["ymax"],
        )


@dataclass
class PipelineState:
    """Persistent state tracking for a download pipeline."""

    last_release: str
    last_run: str  # ISO 8601 datetime string
    theme: str
    type: str
    bbox: BBox | None
    backend: Backend
    output: str  # file path

    def as_dict(self) -> dict:
        """Convert state to a dictionary for JSON serialization."""
        return {
            "last_release": self.last_release,
            "last_run": self.last_run,
            "theme": self.theme,
            "type": self.type,
            "bbox": self.bbox.as_dict() if self.bbox is not None else None,
            "backend": str(self.backend),
            "output": self.output,
        }

    @classmethod
    def from_dict(cls, data: dict) -> PipelineState:
        """Create PipelineState from a dictionary."""
        bbox_data = data.get("bbox")
        return cls(
            last_release=data["last_release"],
            last_run=data["last_run"],
            theme=data["theme"],
            type=data["type"],
            bbox=BBox.from_dict(bbox_data) if bbox_data is not None else None,
            backend=Backend(data["backend"]),
            output=data["output"],
        )
