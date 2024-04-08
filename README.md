# overturemaps-py

Official Python command-line tool of the [Overture Maps Foundation](overturemaps.org)

Note: This repository and project are experimental. Things are likely change including the user interface
until a stable release, but we will keep the documentation here up-to-date.

## Basic Usage

Quick Overview:

```
$ overturemaps download --bbox=-71.058,42.363,-71.068,42.353 -o boston.geojson -f geojson -t building
```

This command will download the building footprints in the specific bounding box
as GeoJSON and save to a file named "boston.geojson"

### Download command options

Download allows the following options:
* `bbox` (optional): west, south, east, north longitude and latitude coordinates. When omitted the
entire dataset for the specified type will be downloaded
* `--output`/`-o` (optional): Location of output file. When omitted output will be written to stdout.
* `-f` (required: one of "geojson", "geojsonseq", "parquet"): output format
* `--type`/`-t` (required): The Overture map data type to be downloaded. Examples of types are `building`
for building footprints, `place` for POI places data, etc. Run `overturemaps download --help` for the
complete list of allowed types

## Installation

`pip install overturemaps`
