# Examples

These scripts demonstrate end-to-end workflows using the overture-toolkit.

## Prerequisites

```bash
pip install -e ".[toolkit]"
```

## `austin_buildings_geoparquet.py`

Initializes a local GeoParquet file with all Overture building footprints in
central Austin, TX, then performs an incremental update to bring it up to the
latest release.

```bash
python examples/austin_buildings_geoparquet.py
```

## `austin_buildings_postgis.py`

Same workflow, but stores data in a PostGIS database.  Requires a running
PostgreSQL instance with PostGIS.

```bash
# Set your database URL
export DATABASE_URL="postgresql://user:pass@localhost/overture_demo"
python examples/austin_buildings_postgis.py
```
