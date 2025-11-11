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

Download the building footprints for the specific bounding box as GeoJSON and save to a file named "boston.geojson"

```
overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

## Usage

#### `download`

Download Overture Maps data with an optional bounding box into the specified file format.
When specifying a bounding box, only the minimum data is transferred. The result is streamed out and
can handle arbitrarily large bounding boxes.

Command-line options:

- `--bbox` (optional): west, south, east, north longitude and latitude coordinates. When omitted the
  entire dataset for the specified type will be downloaded
- `-f` (required: one of "geojson", "geojsonseq", "geoparquet"): output format
- `--output`/`-o` (optional): Location of output file. When omitted output will be written to stdout.
- `--type`/`-t` (required): The Overture map data type to be downloaded. Examples of types are `building`
  for building footprints, `place` for POI places data, etc. Run `overturemaps download --help` for the
  complete list of allowed types
- `--connect_timeout` (optional): Socket connection timeout, in seconds. If omitted, the AWS SDK default value is used (typically 1 second).
- `--request_timeout` (optional): Socket read timeouts on Windows and macOS, in seconds. If omitted, the AWS SDK default value is used (typically 3 seconds). This option is ignored on non-Windows, non-macOS systems.
- `--stac/--no-stac` (optional): By default, the reader uses Overture's [STAC-geoparquet](https://stac-utils.github.io/stac-geoparquet/latest/) catalog to speed up queries. If the `--no-stac` flag is present, the data will be read normally.

This downloads data directly from Overture's S3 bucket without interacting with any other servers.
By including bounding box extents on each row in the Overture distribution, the underlying Parquet
readers use the Parquet summary statistics to download the minimum amount of data
necessary to extract data from the desired region.

To help find bounding boxes of interest, we like this [bounding box tool](https://boundingbox.klokantech.com/)
from [Klokantech](https://www.klokantech.com/). Choose the CSV format and copy the value directly into
the `--bbox` field here.

#### `gers [UUID]`

Look up an ID in the GERS Registry. If the feature is present in the latest release, it will download the feature and write it out in the specified format.

Command-line options:

- `-f` ("geojson", "geojsonseq", "geoparquet"): output format, defaults to geojsonseq for a single feature on one line.
- `--output`/`-o` (optional): Location of output file. When omitted output will be written to stdout.
- `--connect_timeout` (optional): Socket connection timeout, in seconds. If omitted, the AWS SDK default value is used (typically 1 second).
- `--request_timeout` (optional): Socket read timeouts on Windows and macOS, in seconds. If omitted, the AWS SDK default value is used (typically 3 seconds). This option is ignored on non-Windows, non-macOS systems.

## Installation

To install overturemaps from [PyPi](https://pypi.org/project/overturemaps/) using pip

```shell
pip install overturemaps
```

overturemaps is also on [conda-forge](https://anaconda.org/conda-forge/overturemaps) and can be installed using conda, mamba, or pixi. To install overturemaps using conda:

```shell
conda install -c conda-forge overturemaps
```
