# overturemaps-py

Official Python command-line tool of the [Overture Maps Foundation](https://overturemaps.org)

Overture Maps provides free and open geospatial map data, from many different sources and normalized to a
[common schema](https://github.com/OvertureMaps/schema). This tool helps to download Overture data
within a region of interest and converts it to a few different file formats. For more information about accessing
Overture Maps data, see our official documentation site https://docs.overturemaps.org.

Note: This repository and project are experimental. Things are likely change including the user interface
until a stable release, but we will keep the documentation here up-to-date.

## Quick Start

Download the building footprints for the specific bounding box as GeoJSON and save to a file named "boston.geojson"

```
$ overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

## Usage

#### `download`
There is currently one option to the `overturemaps` utility, `download`. It will download Overture Maps data
with an optional bounding box into the specified file format. When specifying a bounding box,
only the minimum data is transferred. The result is streamed out and can handle arbitrarily
large bounding boxes.

Command-line options:
* `--bbox` (optional): west, south, east, north longitude and latitude coordinates. When omitted the
entire dataset for the specified type will be downloaded
* `-f` (required: one of "geojson", "geojsonseq", "geoparquet"): output format
* `--output`/`-o` (optional): Location of output file. When omitted output will be written to stdout.
* `--type`/`-t` (optional): The Overture map data type to be downloaded. Examples of types are `building`
for building footprints, `place` for POI places data, etc. Run `overturemaps download --help` for the
complete list of allowed types

**Overrides** 
By default if -type is supplied it will match related themes and provides the file path. You can override this behaviour to download your own combination you can do that using following two cli arguments
* `--custom-type`/`-cty` (optional): If you want to override the defaults provided and download them on your own you can pass your type here that would be supplied to url later on
* `--custom-theme`/`-cth` (optional): If you want to override the defaults provided and download them on your own you can pass your theme here 

For eg : To download all places in boston inside places theme without restricting it to type
```bash
overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson -o boston_places.geojson -cth places
```

This downloads data directly from Overture's S3 bucket without interacting with any other servers. 
By including bounding box extents on each row in the Overture distribution, the underlying Parquet
readers use the Parquet summary statistics to download the minimum amount of data
necessary to extract data from the desired region.

To help find bounding boxes of interest, we like this [bounding box tool](https://boundingbox.klokantech.com/)
from [Klokantech](https://www.klokantech.com/). Choose the CSV format and copy the value directly into
the `--bbox` field here.


## Installation

`pip install overturemaps`
