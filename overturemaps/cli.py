"""
Overture Maps (overturemaps.org) command line utility.

Currently provides the ability to extract features from an Overture dataset
in a specified bounding box in a few different file formats.

"""

import importlib.metadata
import json
import os
import sys
import uuid

import click
import pyarrow.parquet as pq
import shapely.wkb

from .core import (
    get_all_overture_types,
    get_available_releases,
    get_latest_release,
    record_batch_reader,
    record_batch_reader_from_gers,
    type_theme_map,
)
from .models import BBox, Backend, PipelineState
from .state import get_state_path, save_state, load_state
from datetime import datetime, timezone
from .releases import list_releases, release_exists


def get_writer(output_format, path, schema):
    if output_format == "geojson":
        writer = GeoJSONWriter(path)
    elif output_format == "geojsonseq":
        writer = GeoJSONSeqWriter(path)
    elif output_format == "geoparquet":
        # Update the geoparquet metadata to remove the file-level bbox which
        # will no longer apply to this file. Since we cannot write the field at
        # the end, just remove it as it's optional. Let the per-row bounding
        # boxes do all the work.
        metadata = schema.metadata
        # extract geo metadata
        geo = json.loads(metadata[b"geo"])
        # the spec allows for multiple geom columns
        geo_columns = geo["columns"]
        if len(geo_columns) > 1:
            raise IOError("Expected single geom column but encountered multiple.")
        for geom_col_vals in geo_columns.values():
            # geom level extents "bbox" is optional - remove if present
            # since extracted data will have different extents
            if "bbox" in geom_col_vals:
                geom_col_vals.pop("bbox")
            # add "covering" if there is a row level "bbox" column
            # this facilitates spatial filters e.g. geopandas read_parquet
            if "bbox" in schema.names:
                geom_col_vals["covering"] = {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                }
        metadata[b"geo"] = json.dumps(geo).encode("utf-8")
        schema = schema.with_metadata(metadata)
        writer = pq.ParquetWriter(path, schema)
    return writer


class BboxParamType(click.ParamType):
    name = "bbox"

    def convert(self, value, param, ctx):
        try:
            bbox = [float(x.strip()) for x in value.split(",")]
            fail = False
        except ValueError:  # ValueError raised when passing non-numbers to float()
            fail = True

        if fail or len(bbox) != 4:
            self.fail(
                f"bbox must be 4 floating point numbers separated by commas. Got '{value}'"
            )

        return bbox


def validate_release(ctx, param, value):
    """Callback to validate release parameter against available releases."""
    if value is None:
        return get_latest_release()

    available_releases, _ = get_available_releases()
    if value not in available_releases:
        raise click.BadParameter(
            f"Release '{value}' not found. Available releases: {', '.join(available_releases)}"
        )
    return value


def validate_gers_id(ctx, param, value):
    """Callback to validate GERS ID is a valid UUID."""
    if not value:
        raise click.BadParameter("GERS ID cannot be empty")

    try:
        # Try to parse as UUID - this validates the format
        # Convert to standard format with dashes (lowercase with dashes)
        parsed_uuid = uuid.UUID(value)
        return str(parsed_uuid)
    except ValueError:
        raise click.BadParameter(f"GERS ID must be a valid UUID. Got: '{value}'")


@click.group()
@click.version_option(
    version=importlib.metadata.version("overturemaps"),
    prog_name="overturemaps",
)
def cli():
    pass


@cli.command()
@click.option("--bbox", required=False, type=BboxParamType())
@click.option(
    "-f",
    "output_format",
    type=click.Choice(["geojson", "geojsonseq", "geoparquet"]),
    required=True,
)
@click.option("-o", "--output", required=False, type=click.Path())
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(get_all_overture_types()),
    required=True,
)
@click.option(
    "-r",
    "--release",
    default=None,
    callback=validate_release,
    required=False,
    help="Release version (defaults to latest)",
)
@click.option(
    "--stac/--no-stac",
    required=False,
    type=bool,
    is_flag=True,
    default=True,
    help="If set, directly read from the dataset path instead of using the STAC-geoparquet index.",
)
@click.option("--connect_timeout", required=False, type=int)
@click.option("--request_timeout", required=False, type=int)
def download(
    bbox, output_format, output, type_, release, connect_timeout, request_timeout, stac
):
    if output_format == "geoparquet" and output is None:
        raise click.UsageError(
            "Output file (-o/--output) is required when using geoparquet format"
        )

    if output is None:
        output_file = sys.stdout
    else:
        output_file = output

    reader = record_batch_reader(
        type_, bbox, release, connect_timeout, request_timeout, stac
    )

    if reader is None:
        return

    with get_writer(output_format, output_file, schema=reader.schema) as writer:
        copy(reader, writer)

    # Save state file if output was written to a file
    if output is not None:
        output_path = os.path.abspath(os.path.expanduser(output))

        # Determine backend from output format
        backend = Backend(output_format)

        # Get theme from type
        theme = type_theme_map.get(type_)
        if theme is None:
            click.echo(f"Warning: Could not determine theme for type {type_}", err=True)
            return

        # Create and save state
        state = PipelineState(
            last_release=release,
            last_run=datetime.now(timezone.utc).isoformat(),
            theme=theme,
            type=type_,
            bbox=(
                BBox(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])
                if bbox is not None
                else None
            ),
            backend=backend,
            output=output_path,
        )

        state_path = get_state_path(output)
        save_state(state, state_path)
        click.echo(f"State saved to {state_path}", err=True)


@cli.command()
@click.argument("gers_id", required=True, callback=validate_gers_id)
@click.option(
    "-f",
    "output_format",
    type=click.Choice(["geojson", "geojsonseq", "geoparquet"]),
    default=None,
    required=False,
    help="Output format. If not specified, only registry information will be displayed.",
)
@click.option("-o", "--output", required=False, type=click.Path())
@click.option("--connect_timeout", required=False, type=int)
@click.option("--request_timeout", required=False, type=int)
@click.pass_context
def gers(ctx, gers_id, output_format, output, connect_timeout, request_timeout):
    """
    Query the GERS registry for a feature by its GERS ID.

    By default, this command only queries the registry and displays
    information about the feature (version, filepath, bbox, etc.) without
    downloading the feature data.

    To download the actual feature data, specify an output format using -f/--format.
    """
    from .core import query_gers_registry

    # First, query the registry to get feature information
    result = query_gers_registry(gers_id)

    if result is None:
        # Error message already printed by query_gers_registry
        ctx.exit(1)

    # If no format specified, we're done - just show the registry info
    if output_format is None:
        click.echo(f"\nRegistry lookup complete for GERS ID: {gers_id}", err=True)
        click.echo("To download the feature data, use -f/--format option.", err=True)
        return

    # Format specified - proceed to download the feature
    if output_format == "geoparquet" and output is None:
        raise click.UsageError(
            "Output file (-o/--output) is required when using geoparquet format"
        )

    if output is None:
        output = sys.stdout

    # Pass the registry result to avoid duplicate query
    reader = record_batch_reader_from_gers(
        gers_id, connect_timeout, request_timeout, registry_result=result
    )

    if reader is None:
        click.echo(
            f"Could not fetch feature data for GERS ID '{gers_id}'",
            err=True,
        )
        ctx.exit(1)

    with get_writer(output_format, output, schema=reader.schema) as writer:
        copy(reader, writer)


def copy(reader, writer):
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows > 0:
            writer.write_batch(batch)


class BaseGeoJSONWriter:
    """
    A base feature writer that manages either a file handle
    or output stream. Subclasses should implement write_feature()
    and finalize() if needed
    """

    def __init__(self, where):
        self.file_handle = None
        if isinstance(where, str):
            self.file_handle = open(os.path.expanduser(where), "w")
            self.writer = self.file_handle
        else:
            self.writer = where
        self.is_open = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        self.close()

    def close(self):
        if not self.is_open:
            return
        self.finalize()
        if self.file_handle:
            self.file_handle.close()
        self.is_open = False

    def write_batch(self, batch):
        if batch.num_rows == 0:
            return

        for row in batch.to_pylist():
            feature = self.row_to_feature(row)
            self.write_feature(feature)

    def write_feature(self, feature):
        pass

    def finalize(self):
        pass

    def row_to_feature(self, row):
        geometry = shapely.wkb.loads(row.pop("geometry"))
        row.pop("bbox")

        # This only removes null values in the top-level dictionary but will leave in
        # nulls in sub-properties
        properties = {k: v for k, v in row.items() if k != "bbox" and v is not None}
        return {
            "type": "Feature",
            "geometry": geometry.__geo_interface__,
            "properties": properties,
        }


class GeoJSONSeqWriter(BaseGeoJSONWriter):
    def write_feature(self, feature):
        self.writer.write(json.dumps(feature, separators=(",", ":")))
        self.writer.write("\n")


class GeoJSONWriter(BaseGeoJSONWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._has_written_feature = False

        self.writer.write('{"type": "FeatureCollection", "features": [\n')

    def write_feature(self, feature):
        if self._has_written_feature:
            self.writer.write(",\n")
        self.writer.write(json.dumps(feature, separators=(",", ":")))
        self._has_written_feature = True

    def finalize(self):
        self.writer.write("]}")


@cli.group()
def releases():
    """Manage and query Overture Maps releases."""
    pass


@releases.command(name="list")
def releases_list():
    """List all available Overture Maps releases."""
    all_releases = list_releases()
    if not all_releases:
        click.echo("No releases found.", err=True)
        return
    for release in all_releases:
        click.echo(release)


@releases.command(name="latest")
def releases_latest():
    """Show the latest Overture Maps release."""
    latest = get_latest_release()
    click.echo(latest)


@releases.command(name="check")
@click.option("-o", "--output", required=True, type=click.Path(exists=True))
@click.pass_context
def releases_check(ctx, output):
    """Check if a local file is up to date with the latest release."""
    state_path = get_state_path(output)
    state = load_state(state_path)

    if state is None:
        click.echo(f"No state file found at {state_path}", err=True)
        click.echo("Cannot determine current release version.", err=True)
        ctx.exit(1)

    latest = get_latest_release()

    click.echo(f"Current release: {state.last_release}")
    click.echo(f"Latest release:  {latest}")

    if state.last_release == latest:
        click.echo("✓ Up to date")
        ctx.exit(0)
    else:
        click.echo("✗ Update available")
        ctx.exit(1)


@releases.command(name="exists")
@click.argument("release")
def releases_exists(release):
    """Check whether a release exists."""
    if not release_exists(release):
        raise click.ClickException(f"Release '{release}' not found")
    click.echo("true")


if __name__ == "__main__":
    cli()
