"""GeoJSON Sequence file backend for local Overture data storage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import pandas as pd
from shapely import wkb

from .base import BaseBackend

if TYPE_CHECKING:
    import pyarrow as pa


class GeoJSONSeqBackend(BaseBackend):
    """Store Overture features in a GeoJSON Sequence (newline-delimited) file.

    Each line contains a single GeoJSON feature. The file is read into memory
    on each operation and written back after mutations. Suitable for datasets
    that comfortably fit in RAM.

    Args:
        path: Path to the GeoJSONSeq file. Created on first upsert if absent.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def write_from_reader(self, reader: "pa.RecordBatchReader") -> int:
        """Bulk write features from a RecordBatchReader (streaming, memory-efficient).

        This writes one GeoJSON feature per line, streaming batches without loading
        everything into memory.

        Args:
            reader: PyArrow RecordBatchReader with feature data.

        Returns:
            Number of features written.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0

        try:
            with open(self.path, "w") as f:
                for batch in reader:
                    if batch.num_rows == 0:
                        continue

                    # Convert batch to dictionary for efficient iteration
                    batch_dict = batch.to_pydict()
                    geometry_bytes = batch_dict.get("geometry", [])
                    num_rows = len(geometry_bytes)

                    for i in range(num_rows):
                        # Parse WKB geometry
                        geom_wkb = geometry_bytes[i]
                        if geom_wkb:
                            geom = wkb.loads(bytes(geom_wkb))
                            geom_dict = geom.__geo_interface__
                        else:
                            geom_dict = None

                        # Build properties dict from all non-geometry columns
                        properties = {}
                        for key, values in batch_dict.items():
                            if key != "geometry":
                                val = values[i]
                                # Skip None values and convert complex types to strings
                                if val is not None:
                                    if isinstance(val, (dict, list)):
                                        properties[key] = val
                                    else:
                                        properties[key] = val

                        # Write GeoJSON feature
                        feature = {
                            "type": "Feature",
                            "geometry": geom_dict,
                            "properties": properties,
                        }
                        f.write(json.dumps(feature, separators=(",", ":")) + "\n")
                        total_rows += 1

            return total_rows

        except Exception as e:
            raise RuntimeError(f"Error writing GeoJSONSeq file: {e}") from e

    def _read(self) -> gpd.GeoDataFrame:
        """Read the current GeoDataFrame from disk, or return empty GDF."""
        if not self.path.exists():
            return gpd.GeoDataFrame()

        try:
            features = []
            with open(self.path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        features.append(json.loads(line))

            if not features:
                return gpd.GeoDataFrame()

            # Convert list of GeoJSON features to GeoDataFrame
            return gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
        except Exception:
            return gpd.GeoDataFrame()

    def _write(self, gdf: gpd.GeoDataFrame) -> None:
        """Write a GeoDataFrame to the GeoJSONSeq file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if gdf.empty:
            # Write empty file
            self.path.write_text("")
            return

        with open(self.path, "w") as f:
            for _, row in gdf.iterrows():
                # Convert row to GeoJSON feature
                feature = {
                    "type": "Feature",
                    "geometry": row.geometry.__geo_interface__,
                    "properties": {
                        k: v
                        for k, v in row.to_dict().items()
                        if k != "geometry" and v is not None
                    },
                }
                f.write(json.dumps(feature, separators=(",", ":")) + "\n")

    def upsert(self, features: gpd.GeoDataFrame) -> None:
        """Insert or update features.

        Existing rows with matching 'id' values are replaced; new rows
        are appended.

        Args:
            features: GeoDataFrame with an 'id' column.
        """
        if features.empty:
            return

        existing = self._read()

        if existing.empty:
            self._write(features)
            return

        # Remove rows whose IDs appear in the incoming data, then append
        ids_to_replace = set(features["id"].tolist())
        filtered = existing[~existing["id"].isin(ids_to_replace)]
        combined = pd.concat([filtered, features], ignore_index=True)

        # Preserve CRS
        if features.crs is not None:
            combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=features.crs)
        else:
            combined = gpd.GeoDataFrame(combined, geometry="geometry")

        self._write(combined)

    def delete(self, ids: set[str]) -> None:
        """Remove features by ID.

        Args:
            ids: Set of feature IDs to remove.
        """
        if not ids:
            return
        gdf = self._read()
        if gdf.empty:
            return
        gdf = gdf[~gdf["id"].isin(ids)]
        self._write(gdf)

    def count(self) -> int:
        """Return the number of features stored locally.

        Returns:
            Row count.
        """
        gdf = self._read()
        return len(gdf)

    def get_feature(self, id: str) -> dict | None:
        """Retrieve a single feature as a dictionary.

        Args:
            id: Feature ID.

        Returns:
            Dict of feature attributes, or None if not found.
        """
        gdf = self._read()
        if gdf.empty:
            return None
        row = gdf[gdf["id"] == id]
        if row.empty:
            return None
        return row.iloc[0].to_dict()

    def get_all_ids(self) -> set[str]:
        """Retrieve all feature IDs currently in the store.

        Returns:
            Set of all feature IDs.
        """
        gdf = self._read()
        if gdf.empty:
            return set()
        return set(gdf["id"].tolist())
