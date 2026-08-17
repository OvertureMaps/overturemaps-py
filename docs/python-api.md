# Python API guide

`overturemaps` is also a Python library. Import directly from `overturemaps` to query Overture data without using the CLI.

## record_batch_reader

`record_batch_reader` returns a `pyarrow.RecordBatchReader`. This is the lowest-level entry point and works with Arrow-compatible tooling.

```python
from overturemaps import record_batch_reader

bbox = (-71.068, 42.353, -71.058, 42.363)
reader = record_batch_reader("building", bbox=bbox)

if reader is not None:
    table = reader.read_all()
    print(table.schema)
```

## geodataframe

`geodataframe` loads data into a `geopandas.GeoDataFrame`.

Install optional dependencies with either `pip install overturemaps[geopandas]` or `pip install geopandas`.

```python
from overturemaps import geodataframe

bbox = (-71.068, 42.353, -71.058, 42.363)
gdf = geodataframe("building", bbox=bbox)
print(gdf.head())
```

## Writing formats

Use `get_writer` and `copy` from `overturemaps.writers` to write data to GeoJSON, GeoJSONSeq, or GeoParquet without using the CLI.

```python
from overturemaps import record_batch_reader
from overturemaps.writers import copy, get_writer

bbox = (-71.068, 42.353, -71.058, 42.363)
reader = record_batch_reader("building", bbox=bbox)

with get_writer("geojson", "boston.geojson", schema=reader.schema) as writer:
    copy(reader, writer)
```

Supported format strings: `geojson`, `geojsonseq`, `geoparquet`.
