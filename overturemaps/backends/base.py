"""Abstract base class for Overture data storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import geopandas as gpd

if TYPE_CHECKING:
    import pyarrow as pa


class BaseBackend(ABC):
    """Abstract interface for local Overture data storage backends.

    All backends must implement write_from_reader for bulk streaming writes,
    plus upsert, delete, count, and get_feature for incremental updates.
    """

    @abstractmethod
    def write_from_reader(self, reader: "pa.RecordBatchReader") -> int:
        """Bulk write features from a RecordBatchReader (streaming, memory-efficient).

        This method is used for initial downloads and full refreshes. It should
        overwrite any existing data.

        Args:
            reader: PyArrow RecordBatchReader with feature data.

        Returns:
            Number of features written.
        """
        ...

    @abstractmethod
    def upsert(self, features: gpd.GeoDataFrame) -> None:
        """Insert or update features in the local store.

        Features with existing IDs are updated; new IDs are inserted.

        Args:
            features: GeoDataFrame of features to upsert (must have an 'id' column).
        """
        ...

    @abstractmethod
    def delete(self, ids: set[str]) -> None:
        """Remove features by their IDs.

        Args:
            ids: Set of feature IDs to delete.
        """
        ...

    @abstractmethod
    def count(self) -> int:
        """Return the total number of features in the local store.

        Returns:
            Feature count.
        """
        ...

    @abstractmethod
    def get_feature(self, id: str) -> dict | None:
        """Retrieve a single feature by ID as a dictionary.

        Args:
            id: Feature ID to look up.

        Returns:
            Feature attributes as a dict, or None if not found.
        """
        ...

    @abstractmethod
    def get_all_ids(self) -> set[str]:
        """Retrieve all feature IDs currently in the store.

        Returns:
            Set of all feature IDs.
        """
        ...

    @abstractmethod
    def check_existing_ids(self, ids: set[str]) -> set[str]:
        """Check which IDs from the given set exist in the store.

        This is more efficient than get_all_ids() when checking specific IDs,
        especially for large backends like PostGIS.

        Args:
            ids: Set of feature IDs to check.

        Returns:
            Subset of input IDs that exist in the store.
        """
        ...
