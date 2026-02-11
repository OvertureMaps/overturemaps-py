[![PyPi](https://img.shields.io/pypi/v/overturemaps.svg)](https://pypi.python.org/pypi/overturemaps)

# overturemaps-py

Official Python command-line tool of the [Overture Maps Foundation](https://overturemaps.org)

Overture Maps provides free and open geospatial map data, from many different sources and normalized to a
[common schema](https://github.com/OvertureMaps/schema). This tool helps to download Overture data
within a region of interest and converts it to a few different file formats. For more information about accessing
Overture Maps data, see our official documentation site <https://docs.overturemaps.org>.

Note: This repository and project are experimental. Things are likely change including the user interface
until a stable release, but we will keep the documentation here up-to-date.

## Quick Start

Download the building footprints for a specific bounding box as GeoJSON:

```bash
overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

This automatically creates a state file (`boston.geojson.state`) that enables future incremental updates.

## Usage

### `download`

Download Overture Maps data with an optional bounding box into the specified file format.
Data is downloaded using memory-efficient streaming, allowing you to work with arbitrarily large datasets.

Command-line options:

- `--bbox` (optional): west, south, east, north longitude and latitude coordinates. Required when using backend mode with `-o` or `--db-url`
- `-f` (optional): output format - one of "geojson", "geojsonseq", "geoparquet", or "postgis". Defaults to "geoparquet"
- `--output`/`-o` (optional): Location of output file. When omitted, output is written to stdout. When specified, enables backend mode with automatic state tracking
- `--db-url` (optional): Database URL for postgis format (e.g., "postgresql://user:pass@localhost/db")
- `--type`/`-t` (required): The Overture map data type to be downloaded. Examples of types are `building`
  for building footprints, `place` for POI places data, etc. Run `overturemaps download --help` for the
  complete list of allowed types
- `--release`/`-r` (optional): Specific release version to download. Defaults to the latest release
- `--connect_timeout` (optional): Socket connection timeout, in seconds. If omitted, the AWS SDK default value is used (typically 1 second)
- `--request_timeout` (optional): Socket read timeouts on Windows and macOS, in seconds. If omitted, the AWS SDK default value is used (typically 3 seconds). This option is ignored on non-Windows, non-macOS systems
- `--stac/--no-stac` (optional): By default, the reader uses Overture's [STAC catalog](https://stac.overturemaps.org/) to speed up queries to the latest release. If the `--no-stac` flag is present, the CLI will use the S3 path for the latest release directly

This downloads data directly from Overture's S3 bucket without interacting with any other servers.
The underlying readers use Parquet summary statistics to download only the minimum amount of data
necessary to extract data from the desired region.

To help find bounding boxes of interest, we like this [bounding box tool](https://boundingbox.klokantech.com/)
from [Klokantech](https://www.klokantech.com/). Choose the CSV format and copy the value directly into
the `--bbox` field here.

### `gers [UUID]`

Look up an ID in the GERS Registry. If the feature is present in the latest release, it will download the feature and write it out in the specified format.

Command-line options:

- `-f` ("geojson", "geojsonseq", "geoparquet"): output format, defaults to geojsonseq for a single feature on one line.
- `--output`/`-o` (optional): Location of output file. When omitted output will be written to stdout.
- `--connect_timeout` (optional): Socket connection timeout, in seconds. If omitted, the AWS SDK default value is used (typically 1 second).
- `--request_timeout` (optional): Socket read timeouts on Windows and macOS, in seconds. If omitted, the AWS SDK default value is used (typically 3 seconds). This option is ignored on non-Windows, non-macOS systems.

## Incremental Updates

The CLI provides incremental update capabilities using the [GERS changelog](https://docs.overturemaps.org/gers/). Instead of re-downloading entire datasets each month, you can fetch only the features that changed between releases and apply them to your local files or PostGIS database.

### Installation

Incremental update functionality and PostGIS support require additional dependencies. Install with:

```bash
pip install overturemaps[toolkit]
```

This installs all optional dependencies including DuckDB, SQLAlchemy, GeoAlchemy2, psycopg2-binary, and others needed for incremental updates and PostGIS storage.

### Updates Quick Start

Initialize a local dataset for a specific area:

```bash
overturemaps download \
  --bbox=-97.8,30.2,-97.6,30.4 \
  --type=building \
  -f geoparquet \
  -o ~/data/austin_buildings.parquet
```

This automatically creates a state file at `~/data/austin_buildings.parquet.state` that tracks your dataset.

On subsequent releases, update incrementally:

```bash
overturemaps update run -o ~/data/austin_buildings.parquet
```

All parameters (theme, type, bbox, format) are automatically read from the state file. You can override any of them on the command line if needed:

```bash
# Update to a specific release
overturemaps update run -o ~/data/austin_buildings.parquet --release=2026-01-21.0

# Override bbox to expand the area
overturemaps update run -o ~/data/austin_buildings.parquet --bbox=-98.0,30.0,-97.5,30.5
```

### Commands

#### `overturemaps releases`

List and query available Overture releases:

- `releases list` - Show all releases
- `releases latest` - Get the most recent release
- `releases check` - Check if your local data is up to date (requires `-o` or `--db-url` to locate state file)

#### `overturemaps download`

Download Overture Maps data with optional bounding box filtering.

**Two modes:**

1. **Direct mode**: Stream data to stdout (when no output location is specified)
2. **Backend mode**: Save to file or database with automatic state tracking (when `-o` or `--db-url` is specified)

Backend mode uses memory-efficient streaming and automatically creates a state file that enables incremental updates. The state file is saved as:

- File formats: `{output}.state` (e.g., `data.parquet.state`)
- PostGIS: `~/.overture/postgis/state_{hash}.json`

Supports geojson, geojsonseq, geoparquet, and postgis formats.

#### `overturemaps changelog`

Query the GERS changelog to see what changed. Results are displayed in formatted tables with color-coded statistics:

- `changelog query` - Get changes within a bounding box (shows added, modified, removed features)
- `changelog summary` - Get aggregate statistics across entire themes/types

Both commands support optional filtering by `--theme` and/or `--type`, and default to the latest release if `--release` is not specified. When querying multiple themes or types, results are grouped in tables with grand totals.

**STAC Acceleration**: The CLI automatically attempts to use Overture's [STAC catalog](https://stac.overturemaps.org/) for accelerated spatial queries. When changelog files are added to the catalog, queries will automatically benefit from the same performance improvements that data downloads currently receive from STAC indexing. Until changelog partitions are added to STAC, queries transparently fall back to direct S3 access.

#### `overturemaps update`

Run incremental updates against your local backend. The `update run` command reads parameters (theme, type, bbox, format) from the state file created by `download`, so you only need to specify the output location:

```bash
# Update to the latest release
overturemaps update run -o ~/data/austin_buildings.parquet

# Update PostGIS database
overturemaps update run --db-url="postgresql://user:pass@localhost/db"

# Update to a specific release
overturemaps update run -o ~/data/austin_buildings.parquet --release=2026-01-21.0
```

You can override any parameter from the state file by providing it on the command line.

Commands:

- `update run` - Apply changes from the latest release (or specified with `--release`)
- `update status` - Show current pipeline state (requires `-o` or `--db-url` to locate state file)

**Note:** Changelogs are sequential (each release contains changes from the previous release). The tool will warn you if you're skipping releases and prompt for confirmation.

### Storage Backends

The download command supports multiple storage formats, all with automatic state tracking:

**GeoJSON**: Store data in GeoJSON format

```bash
overturemaps download -f geojson --type=building --bbox=-97.8,30.2,-97.6,30.4 -o ~/data/features.geojson
# Creates: features.geojson and features.geojson.state
```

**GeoJSON Sequence**: Store data in newline-delimited GeoJSON

```bash
overturemaps download -f geojsonseq --type=building --bbox=-97.8,30.2,-97.6,30.4 -o ~/data/features.geojsonl
# Creates: features.geojsonl and features.geojsonl.state
```

**GeoParquet**: Store data in Parquet format with geospatial extensions (recommended for large datasets)

```bash
overturemaps download -f geoparquet --type=building --bbox=-97.8,30.2,-97.6,30.4 -o ~/data/features.parquet
# Creates: features.parquet and features.parquet.state
```

**PostGIS**: Store data in a PostgreSQL/PostGIS database (requires `pip install overturemaps[toolkit]`)

```bash
overturemaps download -f postgis --type=building --bbox=-97.8,30.2,-97.6,30.4 --db-url="postgresql://user:pass@localhost/db"
# Creates: PostgreSQL table and ~/.overture/postgis/state_{hash}.json
```

All formats use memory-efficient streaming, allowing you to download arbitrarily large datasets without running out of memory.

### Examples

See the [examples/](examples/) directory for complete end-to-end workflows showing:

- Incremental updates with GeoParquet ([austin_buildings_geoparquet.py](examples/austin_buildings_geoparquet.py))
- Incremental updates with PostGIS ([austin_buildings_postgis.py](examples/austin_buildings_postgis.py))

## Installation

To install overturemaps from [PyPi](https://pypi.org/project/overturemaps/) using pip

```bash
pip install overturemaps
```

overturemaps is also on [conda-forge](https://anaconda.org/conda-forge/overturemaps) and can be installed using conda, mamba, or pixi. To install overturemaps using conda:

```bash
conda install -c conda-forge overturemaps
```

If you have [uv](https://docs.astral.sh/uv/) installed, you can run overturemaps [with uvx](https://docs.astral.sh/uv/guides/tools/#running-tools) without installing it:

```bash
uvx overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

## Development

For basic development:
```bash
uv sync
uv run pytest tests/
```

For development with toolkit functionality:
```bash
uv sync --extra toolkit
uv run pytest tests/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed development guidelines.

