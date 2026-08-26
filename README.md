[![PyPI](https://img.shields.io/pypi/v/overturemaps.svg)](https://pypi.org/project/overturemaps/)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/overturemaps.svg)](https://anaconda.org/conda-forge/overturemaps)
[![Homebrew](https://img.shields.io/badge/Homebrew-overturemaps-FBB040?logo=homebrew)](https://formulae.brew.sh/formula/overturemaps)
[![Standalone Binaries](https://img.shields.io/badge/binaries-Linux%20%7C%20macOS%20%7C%20Windows-2ea44f)](https://github.com/OvertureMaps/overturemaps-py/releases/latest)
[![GitHub Release](https://img.shields.io/github/v/release/OvertureMaps/overturemaps-py?display_name=release)](https://github.com/OvertureMaps/overturemaps-py/releases/latest)

# overturemaps-py

<p align="center">
  <img src="docs/screenshot.png" alt="overturemaps cli screenshot" width="800">
</p>

Official Python command-line tool of the [Overture Maps Foundation](https://overturemaps.org).

Overture provides free and open geospatial map data, from many different sources and normalized to a
[common schema](https://github.com/OvertureMaps/schema). This tool helps to download Overture data
within a region of interest and converts it to a few different file formats. For more information about accessing
Overture data, see the [official documentation site](https://docs.overturemaps.org).

## Installation

Install with pip:

```bash
pip install overturemaps
```

Also available via:

```bash
# Homebrew
brew install overturemaps

# conda-forge
conda install -c conda-forge overturemaps

# uvx
uvx overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geoparquet --type=building -o boston.parquet
```


## Quick Start

Download building footprints for a bounding box as GeoParquet:

```bash
overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geoparquet --type=building -o boston.parquet
```

## Commands

The CLI includes these top-level commands:

- `download`: download Overture data, optionally clipped to a bounding box.
- `gers`: look up a single feature by GERS ID.
- `releases`: inspect available Overture releases.
- `changelog`: query GERS feature changes across releases.

Run `overturemaps --help` for the full command tree.

## Documentation

- [CLI guide](docs/cli.md)
- [Python API guide](docs/python-api.md)
- [Performance notes](docs/performance.md)
- [Contributing guide](CONTRIBUTING.md)
