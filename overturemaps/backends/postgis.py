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

    def upsert(self, features: gpd.GeoDataFrame) -> None:
        """Insert or update features using INSERT ... ON CONFLICT DO UPDATE.

        Dynamic columns (beyond id and geometry) are added to the table
        automatically on the first upsert that includes them.

        Args:
            features: GeoDataFrame with 'id' and 'geometry' columns.
        """
        if features.empty:
            return

        # Validate and filter column names to prevent SQL injection
        def is_valid_column_name(name: str) -> bool:
            """Check if column name is safe (alphanumeric and underscore only)."""
            return name.replace("_", "").replace(".", "").isalnum()

        extra_cols = [
            c
            for c in features.columns
            if c not in ("id", "geometry") and is_valid_column_name(c)
        ]

        # Map pandas dtypes to PostgreSQL types
        def infer_pg_type(dtype) -> str:
            """Infer PostgreSQL column type from pandas dtype."""
            dtype_str = str(dtype)
            if dtype_str.startswith("int"):
                return "BIGINT"
            elif dtype_str.startswith("float"):
                return "DOUBLE PRECISION"
            elif dtype_str == "bool":
                return "BOOLEAN"
            elif dtype_str == "object":
                # Could be string, list, dict, etc. - use JSONB for flexibility
                return "JSONB"
            else:
                # Fallback to TEXT for unknown types
                return "TEXT"

        # Ensure any new non-spatial columns exist in the table
        with self._engine.begin() as conn:
            for col in extra_cols:
                try:
                    pg_type = infer_pg_type(features[col].dtype)
                    # Use SQLAlchemy's text() with identifier quoting
                    conn.execute(
                        text(
                            f"ALTER TABLE {self._qualified_table} "
                            f'ADD COLUMN IF NOT EXISTS "{col}" {pg_type};'
                        )
                    )
                except Exception:
                    pass  # Column may already exist or be unsupported

        # Build parameterized INSERT ... ON CONFLICT query
        # Create placeholders for VALUES clause
        value_placeholders = [":id", "ST_GeomFromText(:geom_wkt, 4326)"]
        value_placeholders.extend(f":{col}" for col in extra_cols)

        # Build UPDATE SET clause for ON CONFLICT
        update_set = ["geometry = EXCLUDED.geometry"]
        update_set.extend(f'"{col}" = EXCLUDED."{col}"' for col in extra_cols)

        # Construct SQL with quoted identifiers
        quoted_cols = ['"id"', '"geometry"'] + [f'"{col}"' for col in extra_cols]
        sql = (
            f"INSERT INTO {self._qualified_table} ({', '.join(quoted_cols)}) "
            f"VALUES ({', '.join(value_placeholders)}) "
            f"ON CONFLICT (id) DO UPDATE SET {', '.join(update_set)};"
        )

        # Prepare batch of parameters
        params_batch = []
        for _, row in features.iterrows():
            params = {"id": row["id"]}

            # Handle geometry - convert to WKT or use None
            if row.get("geometry") is not None:
                params["geom_wkt"] = row["geometry"].wkt
            else:
                params["geom_wkt"] = None

            # Add extra column values
            for col in extra_cols:
                val = row.get(col)
                # Handle None values
                if val is None or (isinstance(val, float) and val != val):  # NaN check
                    params[col] = None
                # Convert lists/dicts to JSON string for JSONB columns
                elif isinstance(val, (list, dict)):
                    import json

                    params[col] = json.dumps(val)
                else:
                    params[col] = val

            params_batch.append(params)

        # Execute batch upsert - use executemany for better performance
        with self._engine.begin() as conn:
            # Note: SQLAlchemy's executemany with text() requires multiple execute calls
            # For true batch performance, we'd need to use Core Insert with bindparam
            # But this is still much better than the previous version
            stmt = text(sql)
            for params in params_batch:
                conn.execute(stmt, params)

    def delete(self, ids: set[str]) -> None:
        """Remove features by ID.

        Args:
            ids: Set of feature IDs to delete.
        """
        if not ids:
            return
        # Use parameterized query with ANY() to avoid SQL injection and query length limits
        with self._engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {self._qualified_table} WHERE id = ANY(:ids);"),
                {"ids": list(ids)},
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

        Uses a parameterized query with ANY() to efficiently and safely check IDs.

        Args:
            ids: Set of feature IDs to check.

        Returns:
            Subset of input IDs that exist in the store.
        """
        if not ids:
            return set()

        # Use parameterized query with ANY() - PostgreSQL handles large arrays efficiently
        with self._engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT id FROM {self._qualified_table} WHERE id = ANY(:ids);"),
                {"ids": list(ids)},
            )
            return {row[0] for row in result}
