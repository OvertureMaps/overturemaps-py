"""
Overture Maps (overturemaps.org) command line utility.

Currently provides the ability to extract features from an Overture dataset
in a specified bounding box in a few different file formats.

"""

import importlib.metadata
import os
import sys
import uuid
from datetime import datetime, timezone

import click

from .changelog import query_changelog_ids, summarize_changelog
from .core import (
    get_all_overture_types,
    get_available_releases,
    get_latest_release,
    record_batch_reader,
    record_batch_reader_from_gers,
    type_theme_map,
)
from .models import Backend, BBox, PipelineState
from .releases import list_releases, release_exists
from .state import get_state_path, load_state, save_state
from .writers import copy, get_writer


# Earth's total surface area in square degrees (360 * 180).
EARTH_AREA_SQ_DEG = 64800
# Threshold (fraction of Earth) above which we warn about a large bbox.
LARGE_BBOX_THRESHOLD = 0.01  # 1% of Earth


def _print_banner():
    try:
        import pyfiglet
        banner = pyfiglet.figlet_format("Overture Maps", font="slant")
    except Exception:
        banner = "Overture Maps\n"
    version = importlib.metadata.version("overturemaps")
    click.secho(banner.rstrip(), fg="blue", bold=True, err=True)
    click.secho(f"  v{version}  |  overturemaps.org\n", fg="bright_blue", err=True)


def _bbox_area_sq_deg(xmin: float, ymin: float, xmax: float, ymax: float) -> float:
    """Return the area of a lon/lat bbox in square degrees."""
    return abs(xmax - xmin) * abs(ymax - ymin)


class BboxParamType(click.ParamType):
    name = "bbox"

    def convert(self, value, param, ctx):
        parts = value.split(",")
        if len(parts) != 4:
            self.fail(
                f"bbox requires exactly 4 values (xmin,ymin,xmax,ymax), "
                f"got {len(parts)}. Example: --bbox -71.10,42.34,-71.05,42.36"
            )

        try:
            bbox = [float(x.strip()) for x in parts]
        except ValueError:
            self.fail(
                f"All bbox values must be numbers. Got '{value}'. "
                f"Example: --bbox -71.10,42.34,-71.05,42.36"
            )

        xmin, ymin, xmax, ymax = bbox

        # Validate longitude range
        if not (-180 <= xmin <= 180 and -180 <= xmax <= 180):
            self.fail(
                f"Longitude values must be between -180 and 180. "
                f"Got xmin={xmin}, xmax={xmax}"
            )

        # Validate latitude range
        if not (-90 <= ymin <= 90 and -90 <= ymax <= 90):
            self.fail(
                f"Latitude values must be between -90 and 90. "
                f"Got ymin={ymin}, ymax={ymax}"
            )

        # Check for swapped min/max
        if xmin > xmax:
            self.fail(
                f"xmin ({xmin}) must be less than or equal to xmax ({xmax}). "
                f"bbox format is: xmin,ymin,xmax,ymax"
            )
        if ymin > ymax:
            self.fail(
                f"ymin ({ymin}) must be less than or equal to ymax ({ymax}). "
                f"bbox format is: xmin,ymin,xmax,ymax"
            )

        return bbox


def validate_release(ctx, param, value):
    """Callback to validate release parameter against available releases."""
    if value is None:
        return get_latest_release()

    available_releases, _ = get_available_releases()
    if value not in available_releases:
        raise click.UsageError(
            f"Release '{value}' is no longer available. Overture keeps only the last "
            f"two monthly releases (~60 days) for GDPR compliance. Older releases are "
            f"automatically deleted from AWS S3 and Azure.\n\n"
            f"Available releases: {', '.join(available_releases)}\n"
            f"See all past release notes at: https://docs.overturemaps.org/release-calendar"
        )
    return value


def validate_gers_id(ctx, param, value):
    """Callback to validate GERS ID is a valid UUID."""
    if not value:
        raise click.BadParameter("GERS ID cannot be empty")

    try:
        parsed_uuid = uuid.UUID(value)
        return str(parsed_uuid)
    except ValueError:
        raise click.BadParameter(f"GERS ID must be a valid UUID. Got: '{value}'")


@click.group(invoke_without_command=True)
@click.version_option(
    version=importlib.metadata.version("overturemaps"),
    prog_name="overturemaps",
)
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        _print_banner()
        click.echo(ctx.get_help())


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
    help="By default, uses the STAC catalog to limit which Parquet files are downloaded. Pass --no-stac to skip the catalog and read the full S3 dataset directly.",
)
@click.option("--connect_timeout", required=False, type=int)
@click.option("--request_timeout", required=False, type=int)
def download(
    bbox, output_format, output, type_, release, connect_timeout, request_timeout, stac
):
    if bbox is None:
        click.secho(
            "Warning: No bounding box provided. Downloading the entire dataset "
            "for this type. The full Overture dataset is approximately "
            "1.2 TB as GeoJSON and 400 GB as GeoParquet.",
            fg="yellow",
            bold=True,
            err=True,
        )
    else:
        area = _bbox_area_sq_deg(bbox[0], bbox[1], bbox[2], bbox[3])
        fraction = area / EARTH_AREA_SQ_DEG
        if fraction >= LARGE_BBOX_THRESHOLD:
            pct = fraction * 100
            click.secho(
                f"Warning: The bounding box covers ~{pct:.1f}% of Earth's surface. "
                f"This may take a long time and use significant bandwidth. "
                f"The full Overture dataset is approximately "
                f"1.2 TB as GeoJSON and 400 GB as GeoParquet.",
                fg="yellow",
                bold=True,
                err=True,
            )

    if output_format == "geoparquet" and output is None:
        raise click.UsageError(
            "Output file (-o/--output) is required when using geoparquet format"
        )

    if output is None:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
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

    if output is not None:
        output_path = os.path.abspath(os.path.expanduser(output))
        backend = Backend(output_format)
        theme = type_theme_map.get(type_)
        if theme is None:
            click.secho(
                f"Warning: Could not determine theme for type {type_}",
                fg="yellow",
                bold=True,
                err=True,
            )
            return

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
        click.secho(f"State saved to {state_path}", fg="bright_black", err=True)


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

    result = query_gers_registry(gers_id)

    if result is None:
        ctx.exit(1)

    if output_format is None:
        click.secho(
            f"\nRegistry lookup complete for GERS ID: {gers_id}", fg="bright_black", err=True
        )
        click.secho(
            "To download the feature data, use -f/--format option.", fg="bright_black", err=True
        )
        return

    if output_format == "geoparquet" and output is None:
        raise click.UsageError(
            "Output file (-o/--output) is required when using geoparquet format"
        )

    if output is None:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        output = sys.stdout

    reader = record_batch_reader_from_gers(
        gers_id, connect_timeout, request_timeout, registry_result=result
    )

    if reader is None:
        click.secho(
            f"Could not fetch feature data for GERS ID '{gers_id}'",
            fg="red",
            err=True,
        )
        ctx.exit(1)

    with get_writer(output_format, output, schema=reader.schema) as writer:
        copy(reader, writer)


@cli.group()
def releases():
    """Manage and query Overture Maps releases."""
    pass


@releases.command(name="list")
def releases_list():
    """List all available Overture Maps releases."""
    all_releases = list_releases()
    if not all_releases:
        click.secho("No releases found.", fg="red", err=True)
        return
    for i, release in enumerate(all_releases):
        if i == 0:
            click.secho(release, fg="cyan", bold=True)  # latest
        else:
            click.echo(release)


@releases.command(name="latest")
def releases_latest():
    """Show the latest Overture Maps release."""
    latest = get_latest_release()
    click.secho(latest, fg="cyan", bold=True)


@cli.group()
def changelog():
    """Query the GERS changelog for feature changes."""
    pass


@changelog.command(name="query")
@click.option("--bbox", required=True, type=BboxParamType())
@click.option("--theme", required=False, type=str)
@click.option("--type", "type_", required=False, type=str)
@click.option(
    "-r",
    "--release",
    default=None,
    callback=validate_release,
    required=False,
    help="Release version (defaults to latest)",
)
def changelog_query(bbox, theme, type_, release):
    """Query changelog for changes within a bounding box.

    Examples:
        overturemaps changelog query --bbox=-97.8,30.2,-97.6,30.4 --theme=buildings --type=building
        overturemaps changelog query --bbox=-97.8,30.2,-97.6,30.4 --theme=buildings
    """
    bbox_obj = BBox(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])

    if theme and type_:
        if type_ not in type_theme_map:
            raise click.BadParameter(f"Unknown type '{type_}'", param_hint="--type")
        themes_types = [(theme, type_)]
    elif theme:
        types = [t for t, th in type_theme_map.items() if th == theme]
        themes_types = [(theme, t) for t in types]
    elif type_:
        if type_ not in type_theme_map:
            raise click.BadParameter(f"Unknown type '{type_}'", param_hint="type")
        theme = type_theme_map[type_]
        themes_types = [(theme, type_)]
    else:
        raise click.UsageError("Must specify at least --theme or --type")

    total_added = 0
    total_modified = 0
    total_deleted = 0

    click.secho(f"Querying changelog for release {release}...", fg="bright_black")
    click.echo()

    for theme_name, type_name in themes_types:
        changes = query_changelog_ids(release, theme_name, type_name, bbox_obj)

        added = len(changes.get("added", set()))
        modified = len(changes.get("data_changed", set()))
        deleted = len(changes.get("removed", set()))

        total_added += added
        total_modified += modified
        total_deleted += deleted

        if added + modified + deleted > 0:
            click.secho(f"{theme_name}/{type_name}:", bold=True)
            click.secho(f"  Added:    {added}", fg="green")
            click.secho(f"  Modified: {modified}", fg="yellow")
            click.secho(f"  Deleted:  {deleted}", fg="red")
            click.echo()

    if len(themes_types) > 1:
        click.secho("Total:", bold=True)
        click.secho(f"  Added:    {total_added}", fg="green", bold=True)
        click.secho(f"  Modified: {total_modified}", fg="yellow", bold=True)
        click.secho(f"  Deleted:  {total_deleted}", fg="red", bold=True)


@changelog.command(name="summary")
@click.option("--theme", required=False, type=str)
@click.option("--type", "type_", required=False, type=str)
@click.option(
    "-r",
    "--release",
    default=None,
    callback=validate_release,
    required=False,
    help="Release version (defaults to latest)",
)
def changelog_summary(theme, type_, release):
    """Get aggregate statistics for changelog without bbox filtering.

    Examples:
        overturemaps changelog summary --theme=buildings
        overturemaps changelog summary --type=building
        overturemaps changelog summary  # All themes/types
    """
    click.secho(f"Summarizing changelog for release {release}...", fg="bright_black")
    click.echo()

    try:
        results = summarize_changelog(release, theme, type_)
    except ValueError as e:
        raise click.BadParameter(str(e))

    grand_totals = {}

    for theme_name, types_data in results.items():
        for type_name, change_counts in types_data.items():
            click.secho(f"{theme_name}/{type_name}:", bold=True)
            for change_type, count in sorted(change_counts.items()):
                fg = {"added": "green", "data_changed": "yellow", "removed": "red"}.get(
                    change_type
                )
                click.secho(f"  {change_type}: {count}", fg=fg)
                grand_totals[change_type] = grand_totals.get(change_type, 0) + count
            click.echo()

    if len(results) > 1 or (len(results) == 1 and len(list(results.values())[0]) > 1):
        click.secho("Grand Total:", bold=True)
        for change_type, count in sorted(grand_totals.items()):
            fg = {"added": "green", "data_changed": "yellow", "removed": "red"}.get(
                change_type
            )
            click.secho(f"  {change_type}: {count}", fg=fg, bold=True)


@releases.command(name="check")
@click.option("-o", "--output", required=True, type=click.Path(exists=True))
@click.pass_context
def releases_check(ctx, output):
    """Check if a local file is up to date with the latest release."""
    state_path = get_state_path(output)
    state = load_state(state_path)

    if state is None:
        click.secho(f"No state file found at {state_path}", fg="red", err=True)
        click.secho("Cannot determine current release version.", fg="red", err=True)
        ctx.exit(1)

    latest = get_latest_release()

    click.echo(
        "Current release: " + click.style(state.last_release, fg="cyan", bold=True)
    )
    click.echo("Latest release:  " + click.style(latest, fg="cyan", bold=True))

    if state.last_release == latest:
        click.secho("✓ Up to date", fg="green", bold=True)
        ctx.exit(0)
    else:
        click.secho("✗ Update available", fg="yellow", bold=True)
        ctx.exit(1)


@releases.command(name="exists")
@click.argument("release")
def releases_exists(release):
    """Check whether a release exists."""
    if not release_exists(release):
        raise click.ClickException(f"Release '{release}' not found")
    click.secho("true", fg="green")


if __name__ == "__main__":
    cli()
