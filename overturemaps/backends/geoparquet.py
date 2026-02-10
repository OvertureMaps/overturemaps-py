"""GeoParquet file backend for local Overture data storage."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

from .base import BaseBackend

if TYPE_CHECKING:
    import pyarrow as pa


class GeoParquetBackend(BaseBackend):
    """Store Overture features in a local GeoParquet file.

    The file is read into memory on each operation and written back after
    mutations.  Suitable for datasets that comfortably fit in RAM.

    Args:
        path: Path to the GeoParquet file.  Created on first upsert if absent.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def write_from_reader(self, reader: "pa.RecordBatchReader") -> int:
        """Bulk write features from a RecordBatchReader (streaming, memory-efficient).

        This writes the parquet file directly from the reader using PyArrow's
        ParquetWriter, which is memory-efficient for large datasets.

        Args:
            reader: PyArrow RecordBatchReader with feature data.

        Returns:
            Number of features written.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        writer = None

        try:
            for batch in reader:
                if batch.num_rows == 0:
                    continue

                if writer is None:
                    # Initialize writer with the first batch's schema
                    writer = pq.ParquetWriter(self.path, batch.schema)

                writer.write_batch(batch)
                total_rows += batch.num_rows

            if writer is not None:
                writer.close()

            return total_rows

        except Exception as e:
            if writer is not None:
                writer.close()
            raise RuntimeError(f"Error writing GeoParquet file: {e}") from e

    def _read(self) -> gpd.GeoDataFrame:
        """Read the current GeoDataFrame from disk, or return empty GDF."""
        if self.path.exists():
            return gpd.read_parquet(self.path)
        return gpd.GeoDataFrame()

    def _write(self, gdf: gpd.GeoDataFrame) -> None:
        """Write a GeoDataFrame to the parquet file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(self.path, index=False)

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

        # If all existing rows were replaced, just write the new features
        if filtered.empty:
            self._write(features)
            return

        # Ensure CRS compatibility before concatenation
        # Convert existing data to match the CRS of incoming features
        if features.crs is not None and filtered.crs is not None:
            if filtered.crs != features.crs:
                filtered = filtered.to_crs(features.crs)

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
