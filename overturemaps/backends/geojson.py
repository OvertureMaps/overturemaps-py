"""GeoJSON file backend for local Overture data storage."""

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


class GeoJSONBackend(BaseBackend):
    """Store Overture features in a local GeoJSON file.

    The file is read into memory on each operation and written back after
    mutations. Suitable for datasets that comfortably fit in RAM.

    Args:
        path: Path to the GeoJSON file. Created on first upsert if absent.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def write_from_reader(self, reader: "pa.RecordBatchReader") -> int:
        """Bulk write features from a RecordBatchReader (streaming, memory-efficient).

        This writes a GeoJSON FeatureCollection, streaming features without loading
        everything into memory.

        Args:
            reader: PyArrow RecordBatchReader with feature data.

        Returns:
            Number of features written.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        first_feature = True

        try:
            with open(self.path, "w") as f:
                # Write FeatureCollection opening
                f.write('{"type":"FeatureCollection","features":[')

                for batch in reader:
                    if batch.num_rows == 0:
                        continue

                    # Convert batch to dictionary for efficient iteration
                    batch_dict = batch.to_pydict()
                    geometry_bytes = batch_dict.get("geometry", [])
                    num_rows = len(geometry_bytes)

                    for i in range(num_rows):
                        # Add comma separator for all features except the first
                        if not first_feature:
                            f.write(",")
                        first_feature = False

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
                        f.write(json.dumps(feature, separators=(",", ":")))
                        total_rows += 1

                # Write FeatureCollection closing
                f.write("]}")

            return total_rows

        except Exception as e:
            raise RuntimeError(f"Error writing GeoJSON file: {e}") from e

    def _read(self) -> gpd.GeoDataFrame:
        """Read the current GeoDataFrame from disk, or return empty GDF."""
        if self.path.exists():
            try:
                return gpd.read_file(self.path)
            except Exception:
                return gpd.GeoDataFrame()
        return gpd.GeoDataFrame()

    def _write(self, gdf: gpd.GeoDataFrame) -> None:
        """Write a GeoDataFrame to the GeoJSON file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if gdf.empty:
            # Write empty FeatureCollection
            self.path.write_text(
                json.dumps({"type": "FeatureCollection", "features": []})
            )
        else:
            gdf.to_file(self.path, driver="GeoJSON")

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
