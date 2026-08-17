# CLI guide

`overturemaps` downloads Overture data directly from Overture's S3 bucket and exposes a small set of commands for common data access tasks.

## Commands

- `download`: download Overture data, optionally clipped to a bounding box.
- `gers`: look up a single feature by GERS ID.
- `releases`: inspect available Overture releases.
- `changelog`: query GERS feature changes across releases.

Run `overturemaps --help` for the top-level command tree.

## download

`download` streams Overture data in the requested output format. When you provide a bounding box, the Parquet readers use summary statistics to minimize transferred data.

```bash
overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

Options:

- `--bbox`: west, south, east, north longitude and latitude coordinates. When omitted, the entire dataset for the specified type is downloaded.
- `-f`: required output format. Supported values are `geojson`, `geojsonseq`, and `geoparquet`.
- `--output` or `-o`: output file path. When omitted, output is written to stdout.
- `--type` or `-t`: required Overture data type such as `building` or `place`.
- `--connect_timeout`: socket connection timeout in seconds.
- `--request_timeout`: socket read timeout in seconds on Windows and macOS.
- `--stac` or `--no-stac`: choose whether to resolve the latest release through the [STAC catalog](https://stac.overturemaps.org/).

To find a bounding box, [boundingbox.klokantech.com](https://boundingbox.klokantech.com/) can export coordinates in the format the CLI expects.

## gers

`gers` looks up a feature in the GERS Registry and, when present in the latest release, writes it in the requested format.

```bash
overturemaps gers 08f28d1c-bf17-4c5c-bc1b-0f1f7b0d4abc -f geojsonseq
```

Options:

- `-f`: output format. Defaults to `geojsonseq`.
- `--output` or `-o`: output file path. When omitted, output is written to stdout.
- `--connect_timeout`: socket connection timeout in seconds.
- `--request_timeout`: socket read timeout in seconds on Windows and macOS.

## releases

`releases` inspects published Overture releases.

```bash
overturemaps releases list
overturemaps releases latest
overturemaps releases exists 2025-09-24.0
```

Run `overturemaps releases --help` for the full subcommand set.

## changelog

`changelog` queries GERS feature changes across releases.

```bash
overturemaps changelog summary --theme=buildings
overturemaps changelog query --theme=places --bbox=-71.068,42.353,-71.058,42.363
```

Run `overturemaps changelog --help` for the full subcommand set.
