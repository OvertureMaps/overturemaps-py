"""PostGIS database backend for local Overture data storage."""

from __future__ import annotations

from typing import TYPE_CHECKING

import geopandas as gpd
from sqlalchemy import create_engine, text

from .base import BaseBackend

if TYPE_CHECKING:
    import pyarrow as pa


class PostGISBackend(BaseBackend):
    """Store Overture features in a PostGIS table.

    Creates the target table (with geometry column) on first use if it does
    not already exist.

    Args:
        db_url: SQLAlchemy database URL (e.g. "postgresql://user:pass@host/db").
        table: Name of the target table.
        schema: Database schema. Defaults to "public".
    """

    def __init__(self, db_url: str, table: str, schema: str = "public") -> None:
        self.db_url = db_url
        self.table = table
        self.schema = schema
        self._engine = create_engine(db_url)
        self._ensure_table()

    @property
    def _qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def _ensure_table(self) -> None:
        """Create the table with geometry column if it does not exist."""
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self._qualified_table} (
                id TEXT PRIMARY KEY,
                geometry GEOMETRY(Geometry, 4326)
            )
        """
        with self._engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.execute(text(sql))

    def write_from_reader(self, reader: "pa.RecordBatchReader") -> int:
        """Bulk write features from a RecordBatchReader (streaming, memory-efficient).

        This processes batches one at a time and uses GeoDataFrame.to_postgis for
        efficient bulk inserts. The table is truncated first.

        Args:
            reader: PyArrow RecordBatchReader with feature data.

        Returns:
            Number of features written.
        """
        total_rows = 0
        first_batch = True

        try:
            for batch in reader:
                if batch.num_rows == 0:
                    continue

                # Convert batch to GeoDataFrame
                # The batch now has geoarrow.wkb metadata on the geometry field,
                # so GeoPandas can recognize and parse it automatically
                try:
                    gdf = gpd.GeoDataFrame.from_arrow(batch)
                    # Ensure CRS is set
                    if gdf.crs is None:
                        gdf = gdf.set_crs("EPSG:4326")
                except Exception as e:
                    raise ValueError(
                        f"Error converting Arrow batch to GeoDataFrame: {e}"
                    )

                # On first batch, truncate the table and write with replace
                # On subsequent batches, append
                if first_batch:
                    gdf.to_postgis(
                        self.table,
                        self._engine,
                        schema=self.schema,
                        if_exists="replace",
                        index=False,
                    )
                    # Add PRIMARY KEY constraint on id column for upsert support
                    with self._engine.begin() as conn:
                        try:
                            conn.execute(
                                text(
                                    f"ALTER TABLE {self._qualified_table} "
                                    f"ADD PRIMARY KEY (id);"
                                )
                            )
                        except Exception:
                            # May fail if constraint already exists
                            pass
                    first_batch = False
                else:
                    gdf.to_postgis(
                        self.table,
                        self._engine,
                        schema=self.schema,
                        if_exists="append",
                        index=False,
                    )

                total_rows += batch.num_rows

            return total_rows

        except Exception as e:
            raise RuntimeError(f"Error writing to PostGIS: {e}") from e

    @property
    def _qualified_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def _ensure_table(self) -> None:
        """Create the table with geometry column if it does not exist."""
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self._qualified_table} (
                id TEXT PRIMARY KEY,
                geometry GEOMETRY(Geometry, 4326)
            )
        """
        with self._engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            conn.execute(text(sql))

    def upsert(self, features: gpd.GeoDataFrame) -> None:
        """Insert or update features using INSERT ... ON CONFLICT DO UPDATE.

        Dynamic columns (beyond id and geometry) are added to the table
        automatically on the first upsert that includes them.

        Args:
            features: GeoDataFrame with 'id' and 'geometry' columns.
        """
        if features.empty:
            return

        # Ensure any new non-spatial columns exist in the table
        extra_cols = [c for c in features.columns if c not in ("id", "geometry")]
        with self._engine.begin() as conn:
            for col in extra_cols:
                try:
                    conn.execute(
                        text(
                            f"ALTER TABLE {self._qualified_table} "
                            f"ADD COLUMN IF NOT EXISTS {col} TEXT;"
                        )
                    )
                except Exception:
                    pass  # Column may already exist or be unsupported

        # Use GeoDataFrame.to_postgis with if_exists='append', then deduplicate
        # For a proper upsert we build explicit SQL
        with self._engine.begin() as conn:
            for _, row in features.iterrows():
                col_names = ["id", "geometry"] + extra_cols
                placeholders = [f"'{row['id']}'"]
                geom_wkt = (
                    row["geometry"].wkt if row.get("geometry") is not None else "NULL"
                )
                placeholders.append(f"ST_GeomFromText('{geom_wkt}', 4326)")
                for col in extra_cols:
                    val = row.get(col)
                    if val is None:
                        placeholders.append("NULL")
                    else:
                        escaped = str(val).replace("'", "''")
                        placeholders.append(f"'{escaped}'")

                update_set = ", ".join(
                    ["geometry = EXCLUDED.geometry"]
                    + [f"{c} = EXCLUDED.{c}" for c in extra_cols]
                )

                sql = (
                    f"INSERT INTO {self._qualified_table} ({', '.join(col_names)}) "
                    f"VALUES ({', '.join(placeholders)}) "
                    f"ON CONFLICT (id) DO UPDATE SET {update_set};"
                )
                conn.execute(text(sql))

    def delete(self, ids: set[str]) -> None:
        """Remove features by ID.

        Args:
            ids: Set of feature IDs to delete.
        """
        if not ids:
            return
        id_list = ", ".join(f"'{i}'" for i in ids)
        with self._engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {self._qualified_table} WHERE id IN ({id_list});")
            )

    def count(self) -> int:
        """Return the number of rows in the table.

        Returns:
            Row count.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {self._qualified_table};")
            )
            return result.scalar() or 0

    def get_feature(self, id: str) -> dict | None:
        """Retrieve a single feature as a dictionary.

        Args:
            id: Feature ID.

        Returns:
            Dict of feature attributes, or None if not found.
        """
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT * FROM {self._qualified_table} WHERE id = :id;"),
                {"id": id},
            )
            row = result.mappings().fetchone()
            return dict(row) if row else None

    def get_all_ids(self) -> set[str]:
        """Retrieve all feature IDs currently in the store.

        Returns:
            Set of all feature IDs.
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(f"SELECT id FROM {self._qualified_table};"))
            return {row[0] for row in result}

    def check_existing_ids(self, ids: set[str]) -> set[str]:
        """Check which IDs from the given set exist in the store.

        Uses a WHERE IN query to efficiently check only the specified IDs.

        Args:
            ids: Set of feature IDs to check.

        Returns:
            Subset of input IDs that exist in the store.
        """
        if not ids:
            return set()

        # Split into batches to avoid SQL query size limits
        batch_size = 1000
        ids_list = list(ids)
        existing = set()

        for i in range(0, len(ids_list), batch_size):
            batch = ids_list[i : i + batch_size]
            id_list_str = ", ".join(f"'{id_}'" for id_ in batch)

            with self._engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT id FROM {self._qualified_table} WHERE id IN ({id_list_str});"
                    )
                )
                existing.update(row[0] for row in result)

        return existing
