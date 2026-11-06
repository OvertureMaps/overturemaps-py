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

## STAC index

When you pass a `bbox`, `record_batch_reader`, `geodataframe` and `overturemaps.core.count_rows` use the release's [STAC index](https://stac.overturemaps.org/) to read only the files that intersect it. The index is downloaded once per release and reused by later queries in the same process. Rate-limited (HTTP 429) and server error (5xx) responses are retried with backoff.

If the index still can't be read, the query issues a `StacIndexWarning` and falls back to scanning every file of the type. Results are the same, but the scan can take several minutes. To fail instead:

```python
import warnings

from overturemaps.core import StacIndexWarning

warnings.simplefilter("error", StacIndexWarning)
```

Pass `stac=False` to skip the index and scan every file.

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
