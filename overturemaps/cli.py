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
)


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
    type=click.Choice(["geojson", "geojsonseq", "geoparquet", "postgis"]),
    default="geoparquet",
    help="Output format (or storage backend for incremental updates)",
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
@click.option("--db-url", help="Database URL (required when format=postgis)")
def download(
    bbox,
    output_format,
    output,
    type_,
    release,
    connect_timeout,
    request_timeout,
    stac,
    db_url,
):
    """Download Overture Maps data.

    Two modes of operation:

    1. Direct download: Stream data to stdout (no --output specified).

    2. Backend mode: Save to file/database with state tracking for incremental updates.
       Requires --bbox and --output (or --db-url for postgis).
       State is automatically saved to enable future incremental updates.
    """
    # Backend mode: save to file/database with state tracking
    # Triggered when output location is specified (output or db_url)
    if output or db_url:
        _download_to_backend(
            bbox=bbox,
            type_=type_,
            release=release,
            output_format=output_format,
            output=output,
            db_url=db_url,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
            stac=stac,
        )
        return

    # Direct download mode: Stream to stdout
    if output_format == "postgis":
        raise click.UsageError("postgis format requires --db-url")

    if output_format == "geoparquet" and output is None:
        raise click.UsageError(
            "Output file (-o/--output) is required when using geoparquet format"
        )

    if output is None:
        output = sys.stdout

    reader = record_batch_reader(
        type_, bbox, release, connect_timeout, request_timeout, stac
    )

    if reader is None:
        return

    with get_writer(output_format, output, schema=reader.schema) as writer:
        copy(reader, writer)


def _download_to_backend(
    bbox,
    type_,
    release,
    output_format,
    output,
    db_url,
    connect_timeout,
    request_timeout,
    stac,
):
    """Helper function to download data to a storage backend with state tracking."""
    from pathlib import Path
    from datetime import datetime, timezone
    from rich.console import Console
    from .core import record_batch_reader, type_theme_map
    from .state import save_state, get_state_file_for_backend
    from .models import BBox, Backend, PipelineState

    console = Console()

    # Backend mode requires bbox
    if not bbox:
        raise click.UsageError(
            "The --bbox option is required when saving to file/database (backend mode)"
        )

    # Parse bbox into BBox model
    area = BBox(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])

    # Get theme from type
    theme = type_theme_map.get(type_)
    if not theme:
        raise click.UsageError(f"Unknown type: {type_}")

    console.print(f"[bold blue]Downloading from release {release}[/bold blue] …")

    # Get streaming reader with spinner
    with console.status("[bold blue]Connecting to data source...", spinner="dots"):
        try:
            reader = record_batch_reader(
                type_,
                bbox=bbox,
                release=release,
                connect_timeout=connect_timeout,
                request_timeout=request_timeout,
                stac=stac,
            )

            if reader is None:
                console.print("[bold red]Failed to create data reader.[/bold red]")
                sys.exit(1)
        except Exception as e:
            console.print(f"[bold red]Fetch error: {e}[/bold red]")
            sys.exit(1)

    # Resolve backend instance
    backend = Backend(output_format)
    backend_instance = _resolve_backend(
        backend, Path(output) if output else None, db_url
    )

    # Write data using streaming method
    with console.status("[bold blue]Writing data...", spinner="dots"):
        try:
            feature_count = backend_instance.write_from_reader(reader)
        except Exception as e:
            console.print(f"[bold red]Write error: {e}[/bold red]")
            sys.exit(1)

    if feature_count == 0:
        console.print(
            "[bold yellow]No features found in the specified area.[/bold yellow]"
        )
        sys.exit(1)

    # Save state for incremental updates - automatically determine location
    state_path = get_state_file_for_backend(backend, output, db_url)
    state = PipelineState(
        last_release=release,
        last_run=datetime.now(timezone.utc).isoformat(),
        theme=theme,
        type=type_,
        bbox=area,
        backend=backend,
        output=str(output) if output else db_url,
    )
    save_state(state, state_path)

    console.print(
        f"[green]✓ Downloaded {feature_count:,} features (release {release}).[/green]"
    )
    console.print(f"[dim]State saved to: {state_path}[/dim]")


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
def gers(gers_id, output_format, output, connect_timeout, request_timeout):
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
        sys.exit(1)

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
        sys.exit(1)

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


# ---------------------------------------------------------------------------
# Toolkit Commands - Incremental Update Functionality
# ---------------------------------------------------------------------------


@cli.group()
def releases():
    """List and inspect available Overture releases."""
    pass


@releases.command("list")
def releases_list():
    """List all available Overture Maps releases (newest first)."""
    from .releases import list_releases as get_releases_list

    try:
        releases_data = get_releases_list()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if not releases_data:
        click.echo("No releases found.", err=True)
        sys.exit(0)

    for r in releases_data:
        click.echo(r)


@releases.command("latest")
def releases_latest():
    """Print the ID of the most recent Overture Maps release."""
    from .releases import get_latest_release as get_latest

    try:
        latest = get_latest()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo(latest)


@releases.command("check")
@click.option(
    "-f",
    "--format",
    "output_format",
    help="Output format (geojson, geojsonseq, geoparquet, postgis).",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Output file path (for file-based formats).",
)
@click.option("--db-url", help="Database URL (for postgis format).")
def releases_check(output_format, output, db_url):
    """Check whether the local dataset is up to date.

    Requires either --output (for file formats) or --db-url (for postgis) to locate the state file.
    """
    from pathlib import Path
    from .releases import get_latest_release as get_latest
    from .state import load_state, get_state_file_for_backend
    from .models import Backend

    if not output and not db_url:
        click.echo(
            "Error: Must specify either --output or --db-url to check state.", err=True
        )
        sys.exit(1)

    if not output_format:
        # Try to infer format
        if db_url:
            output_format = "postgis"
        elif output:
            # Try to infer from extension
            ext = Path(output).suffix.lower()
            if ext in (".parquet", ".geoparquet"):
                output_format = "geoparquet"
            elif ext == ".geojson":
                output_format = "geojson"
            elif ext == ".geojsonl" or ext == ".jsonl":
                output_format = "geojsonseq"
            else:
                click.echo(
                    "Error: Cannot infer format from output file. Please specify --format.",
                    err=True,
                )
                sys.exit(1)
        else:
            click.echo("Error: Must specify --format when checking state.", err=True)
            sys.exit(1)

    backend = Backend(output_format)
    state_path = get_state_file_for_backend(backend, output, db_url)
    state = load_state(state_path)

    if state is None:
        click.echo(
            f"Error: No state found at {state_path}. Run 'overturemaps download' to initialize.",
            err=True,
        )
        sys.exit(2)

    try:
        latest = get_latest()
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if state.last_release == latest:
        click.echo(f"Up to date (release {latest})")
        sys.exit(0)
    else:
        click.echo(f"Update available: {state.last_release} → {latest}")
        sys.exit(1)


@cli.group()
def changelog():
    """Query the GERS changelog for a release."""
    pass


@changelog.command("query")
@click.option(
    "--release",
    help="Release ID to query (optional, defaults to latest release).",
)
@click.option("--theme", help="Overture theme (optional, defaults to all themes).")
@click.option(
    "--type", "type_", help="Feature type (optional, defaults to all types in theme)."
)
@click.option(
    "--bbox",
    required=True,
    help="Bounding box as comma-separated floats: xmin,ymin,xmax,ymax.",
)
@click.option("-o", "--output", type=click.Path(), help="Save results to this file.")
def changelog_query(release, theme, type_, bbox, output):
    """Query the changelog for changes within a bounding box.

    If --release is not specified, queries the latest release.
    If --theme and --type are not specified, queries all themes and types.
    """
    from rich.console import Console
    from rich.table import Table
    from .changelog import query_changelog_ids_multi
    from .models import BBox
    from .releases import get_latest_release as get_latest

    console = Console()

    # Default to latest release if not specified
    if not release:
        with console.status("[bold blue]Fetching latest release...", spinner="dots"):
            release = get_latest()
        console.print(f"[dim]Using latest release: {release}[/dim]")

    # Parse bbox
    try:
        parts = [float(x.strip()) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError
        area = BBox(*parts)
    except ValueError:
        console.print(
            "[bold red]Error: --bbox requires 4 comma-separated floats: xmin,ymin,xmax,ymax[/bold red]"
        )
        sys.exit(1)

    # Query changelog with spinner
    with console.status(
        f"[bold blue]Querying changelog for {release}...", spinner="dots"
    ):
        try:
            results = query_changelog_ids_multi(release, area, theme, type_)
        except Exception as e:
            console.print(f"[bold red]Error querying changelog: {e}[/bold red]")
            sys.exit(1)

    if output:
        from pathlib import Path

        # Flatten results for output
        output_data = {}
        for theme_name in results:
            for type_name, (ids_add, ids_mod, ids_del) in results[theme_name].items():
                key = f"{theme_name}/{type_name}"
                output_data[key] = {
                    "added": list(ids_add),
                    "modified": list(ids_mod),
                    "removed": list(ids_del),
                }

        Path(output).write_text(json.dumps(output_data, indent=2))

        # Count totals
        total = sum(
            len(ids_add) + len(ids_mod) + len(ids_del)
            for theme_results in results.values()
            for ids_add, ids_mod, ids_del in theme_results.values()
        )
        console.print(f"[green]✓ Saved {total} change IDs to {output}[/green]")
    else:
        # Display results using rich tables
        if theme and type_:
            title = f"Changelog: {release} / {theme} / {type_}"
        elif theme:
            title = f"Changelog: {release} / {theme} / (all types)"
        elif type_:
            title = f"Changelog: {release} / {type_}"
        else:
            title = f"Changelog: {release} (all themes and types)"

        console.print(f"\n[bold]{title}[/bold]\n")

        grand_added = 0
        grand_modified = 0
        grand_removed = 0

        # Single combined table
        table = Table(show_header=True, header_style="bold cyan")

        # Add Theme column only if showing multiple themes or types
        if not (theme and type_):
            table.add_column("Theme", style="magenta", no_wrap=True)
        table.add_column("Type", style="cyan", no_wrap=True)
        table.add_column("Added", justify="right", style="green", no_wrap=True)
        table.add_column("Modified", justify="right", style="yellow", no_wrap=True)
        table.add_column("Removed", justify="right", style="red", no_wrap=True)
        table.add_column("Total", justify="right", style="bold", no_wrap=True)

        # Add all rows to the single table
        for theme_name in sorted(results.keys()):
            for type_name in sorted(results[theme_name].keys()):
                ids_add, ids_mod, ids_del = results[theme_name][type_name]
                total = len(ids_add) + len(ids_mod) + len(ids_del)

                grand_added += len(ids_add)
                grand_modified += len(ids_mod)
                grand_removed += len(ids_del)

                # Build row with or without theme column
                row_data = []
                if not (theme and type_):
                    row_data.append(theme_name)

                # Calculate percentages
                add_pct = (len(ids_add) / total * 100) if total > 0 else 0
                mod_pct = (len(ids_mod) / total * 100) if total > 0 else 0
                del_pct = (len(ids_del) / total * 100) if total > 0 else 0

                row_data.extend(
                    [
                        type_name,
                        f"{len(ids_add):,} ({add_pct:.1f}%)"
                        if len(ids_add) > 0
                        else "-",
                        f"{len(ids_mod):,} ({mod_pct:.1f}%)"
                        if len(ids_mod) > 0
                        else "-",
                        f"{len(ids_del):,} ({del_pct:.1f}%)"
                        if len(ids_del) > 0
                        else "-",
                        f"{total:,}",
                    ]
                )

                table.add_row(*row_data)

        console.print(table)
        console.print()


@changelog.command("summary")
@click.option("--release", help="Release ID (optional, defaults to latest release).")
@click.option("--theme", help="Overture theme (optional, defaults to all themes).")
@click.option(
    "--type", "type_", help="Feature type (optional, defaults to all types in theme)."
)
def changelog_summary(release, theme, type_):
    """Print change counts by type, optionally filtering by theme and/or type.

    If --release is not specified, queries the latest release.
    If neither --theme nor --type is specified, shows change counts for all themes and types.
    If only --theme is specified, shows all types within that theme.
    If only --type is specified, shows just that type.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.spinner import Spinner
    from rich.live import Live
    from .changelog import summarize_changelog
    from .releases import get_latest_release as get_latest

    console = Console()

    # Default to latest release if not specified
    if not release:
        with console.status("[bold blue]Fetching latest release...", spinner="dots"):
            release = get_latest()
        console.print(f"[dim]Using latest release: {release}[/dim]\n")

    # Fetch changelog with spinner
    with console.status(
        f"[bold blue]Querying changelog for {release}...", spinner="dots"
    ):
        try:
            results = summarize_changelog(release, theme, type_)
        except Exception as e:
            console.print(f"[bold red]Error: {e}[/bold red]")
            sys.exit(1)

    # Display results using rich tables
    if theme and type_:
        title = f"Changelog Summary: {release} / {theme} / {type_}"
    elif theme:
        title = f"Changelog Summary: {release} / {theme} / (all types)"
    elif type_:
        title = f"Changelog Summary: {release} / {type_}"
    else:
        title = f"Changelog Summary: {release} (all themes and types)"

    console.print(f"\n[bold]{title}[/bold]\n")

    # Create single combined table
    grand_totals = {"added": 0, "data_changed": 0, "removed": 0, "unchanged": 0}

    # Single table for all results
    table = Table(show_header=True, header_style="bold cyan")

    # Add Theme column only if showing multiple themes or types
    if not (theme and type_):
        table.add_column("Theme", style="magenta", no_wrap=True)
    table.add_column("Type", style="cyan", no_wrap=True)
    table.add_column("Added", justify="right", style="green", no_wrap=True)
    table.add_column("Modified", justify="right", style="yellow", no_wrap=True)
    table.add_column("Removed", justify="right", style="red", no_wrap=True)
    table.add_column("Unchanged", justify="right", style="dim", no_wrap=True)
    table.add_column("Total", justify="right", style="bold", no_wrap=True)

    # Add all rows to the single table
    for theme_name in sorted(results.keys()):
        for type_name in sorted(results[theme_name].keys()):
            change_counts = results[theme_name][type_name]

            added = change_counts.get("added", 0)
            modified = change_counts.get("data_changed", 0)
            removed = change_counts.get("removed", 0)
            unchanged = change_counts.get("unchanged", 0)
            total = added + modified + removed + unchanged

            grand_totals["added"] += added
            grand_totals["data_changed"] += modified
            grand_totals["removed"] += removed
            grand_totals["unchanged"] += unchanged

            # Build row with or without theme column
            row_data = []
            if not (theme and type_):
                row_data.append(theme_name)

            # Calculate percentages
            add_pct = (added / total * 100) if total > 0 else 0
            mod_pct = (modified / total * 100) if total > 0 else 0
            rem_pct = (removed / total * 100) if total > 0 else 0
            unch_pct = (unchanged / total * 100) if total > 0 else 0

            row_data.extend(
                [
                    type_name,
                    f"{added:,} ({add_pct:.1f}%)" if added > 0 else "-",
                    f"{modified:,} ({mod_pct:.1f}%)" if modified > 0 else "-",
                    f"{removed:,} ({rem_pct:.1f}%)" if removed > 0 else "-",
                    f"{unchanged:,} ({unch_pct:.1f}%)" if unchanged > 0 else "-",
                    f"{total:,}",
                ]
            )

            table.add_row(*row_data)

    console.print(table)
    console.print()

    # Print grand totals if showing multiple themes or types
    if not (theme and type_):
        grand_total = sum(grand_totals.values())
        totals_table = Table(show_header=True, header_style="bold magenta")
        totals_table.add_column("Grand Totals", style="bold")
        totals_table.add_column("Count", justify="right", style="bold")

        if grand_totals["added"] > 0:
            totals_table.add_row("Added", f"[green]{grand_totals['added']:,}[/green]")
        if grand_totals["data_changed"] > 0:
            totals_table.add_row(
                "Modified", f"[yellow]{grand_totals['data_changed']:,}[/yellow]"
            )
        if grand_totals["removed"] > 0:
            totals_table.add_row("Removed", f"[red]{grand_totals['removed']:,}[/red]")
        if grand_totals["unchanged"] > 0:
            totals_table.add_row(
                "Unchanged", f"[dim]{grand_totals['unchanged']:,}[/dim]"
            )
        totals_table.add_row("[bold]Total[/bold]", f"[bold]{grand_total:,}[/bold]")

        console.print(totals_table)


@cli.group()
def update():
    """Run incremental updates against a backend."""
    pass


@update.command("run")
@click.option("--theme", help="Overture theme (read from state file if not provided).")
@click.option(
    "--type", "type_", help="Feature type (read from state file if not provided)."
)
@click.option(
    "--bbox",
    help="Bounding box as comma-separated floats: xmin,ymin,xmax,ymax (read from state file if not provided).",
)
@click.option(
    "-f",
    "output_format",
    type=click.Choice(["geojson", "geojsonseq", "geoparquet", "postgis"]),
    help="Storage format (read from state file if not provided).",
)
@click.option(
    "-o", "--output", type=click.Path(), help="Output file (for file-based formats)."
)
@click.option("--db-url", help="Database URL (for postgis format).")
@click.option("--release", help="Target release (default: latest).")
@click.option("--dry-run", is_flag=True, help="Print plan without applying.")
def update_run(theme, type_, bbox, output_format, output, db_url, release, dry_run):
    """Run an incremental update against a local backend.

    Parameters can be provided on the command line or loaded from the state file.
    At minimum, specify -o or --db-url to locate the state file.
    """
    from pathlib import Path
    from datetime import datetime, timezone
    from rich.console import Console
    from .changelog import query_changelog_ids
    from .fetch import fetch_features
    from .releases import get_latest_release as get_latest
    from .state import load_state, save_state, get_state_file_for_backend
    from .models import BBox, Backend, PipelineState

    console = Console()

    # Require either output or db_url to locate state file
    if not output and not db_url:
        console.print(
            "[bold red]Error: Must specify either -o/--output or --db-url to locate the state file.[/bold red]"
        )
        sys.exit(1)

    # Try to determine backend format and locate state file
    if output_format:
        backend = Backend(output_format)
    elif output:
        # Try to guess from file extension
        extension = Path(output).suffix.lower()
        extension_map = {
            ".geojson": Backend.geojson,
            ".json": Backend.geojson,
            ".geojsonl": Backend.geojsonseq,
            ".jsonl": Backend.geojsonseq,
            ".parquet": Backend.geoparquet,
        }
        backend = extension_map.get(extension, Backend.geoparquet)
    elif db_url:
        backend = Backend.postgis
    else:
        click.echo("Error: Cannot determine backend format.", err=True)
        sys.exit(1)

    # Load state file
    state_path = get_state_file_for_backend(backend, output, db_url)
    state = load_state(state_path)

    # Use state file values if parameters not provided
    if state:
        theme = theme or state.theme
        type_ = type_ or state.type
        if not bbox and state.bbox:
            area = state.bbox
        output_format = output_format or state.backend.value
        backend = state.backend
    else:
        # No state file - require all parameters
        if not theme:
            click.echo("Error: --theme is required (no state file found).", err=True)
            sys.exit(1)
        if not type_:
            click.echo("Error: --type is required (no state file found).", err=True)
            sys.exit(1)
        if not bbox:
            click.echo("Error: --bbox is required (no state file found).", err=True)
            sys.exit(1)
        if not output_format:
            click.echo(
                "Error: -f/--format is required (no state file found).", err=True
            )
            sys.exit(1)

    # Parse bbox if provided as string
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError
            area = BBox(*parts)
        except ValueError:
            click.echo(
                "Error: --bbox requires 4 comma-separated floats: xmin,ymin,xmax,ymax",
                err=True,
            )
            sys.exit(1)
    elif not state:
        click.echo("Error: --bbox is required.", err=True)
        sys.exit(1)

    # Resolve target release
    if release is None:
        with console.status("[bold blue]Fetching latest release...", spinner="dots"):
            try:
                release = get_latest()
            except Exception as e:
                console.print(f"[bold red]Error: {e}[/bold red]")
                sys.exit(1)

    # Check if already up to date
    if state is not None and state.last_release == release:
        click.echo(f"Already up to date at release {release}.")
        sys.exit(0)

    # Warn if skipping releases
    if state is not None:
        from .releases import get_next_release

        expected_next = get_next_release(state.last_release)
        if expected_next and expected_next != release:
            click.echo(
                f"⚠️  Warning: Skipping releases between {state.last_release} and {release}",
                err=True,
            )
            click.echo(
                f"   Expected next release: {expected_next}",
                err=True,
            )
            click.echo(
                "   Changelogs are sequential. For best results, update one release at a time.",
                err=True,
            )
            click.echo(
                "   Or use 'download' to fetch the target release directly.",
                err=True,
            )
            if not click.confirm("Continue anyway?"):
                sys.exit(1)

    console.print(f"[bold blue]Updating to release {release}[/bold blue] …")

    # Get backend instance
    backend_instance = _resolve_backend(
        Backend(output_format), Path(output) if output else None, db_url
    )

    # Query changelog with spinner
    with console.status("[bold blue]Querying changelog...", spinner="dots"):
        try:
            ids_to_add, ids_to_modify, ids_to_delete = query_changelog_ids(
                release, theme, type_, area
            )
        except Exception as e:
            console.print(f"[bold red]Changelog error: {e}[/bold red]")
            sys.exit(1)

    # Analyze changelog accuracy
    with console.status("[bold blue]Checking backend state...", spinner="dots"):
        ids_to_check = ids_to_delete | ids_to_add
        existing_ids = backend_instance.check_existing_ids(ids_to_check)

    false_positive_removes = ids_to_delete - existing_ids
    false_positive_adds = ids_to_add & existing_ids

    console.print(
        f"[dim]Changes: +{len(ids_to_add)} ~{len(ids_to_modify)} -{len(ids_to_delete)}[/dim]"
    )

    if false_positive_removes or false_positive_adds:
        console.print("[yellow]⚠️  Changelog anomalies detected:[/yellow]")
        if false_positive_removes:
            console.print(
                f"[dim]   {len(false_positive_removes)} features marked 'removed' but not in current dataset[/dim]"
            )
        if false_positive_adds:
            console.print(
                f"[dim]   {len(false_positive_adds)} features marked 'added' but already in current dataset[/dim]"
            )
        console.print("[dim]   (This is normal due to bbox overlap filtering)[/dim]")

    ids_to_fetch = ids_to_add | ids_to_modify

    if dry_run:
        console.print("[bold yellow]Dry run — no changes applied.[/bold yellow]")
        sys.exit(0)

    # Fetch updated features with spinner
    features = None
    if ids_to_fetch:
        with console.status(
            f"[bold blue]Fetching {len(ids_to_fetch):,} features from S3...",
            spinner="dots",
        ):
            try:
                features = fetch_features(release, theme, type_, ids_to_fetch, area)
            except Exception as e:
                console.print(f"[bold red]Fetch error: {e}[/bold red]")
                sys.exit(1)

    # Apply changes to backend with spinner
    with console.status("[bold blue]Applying changes to backend...", spinner="dots"):
        if features is not None and not features.empty:
            backend_instance.upsert(features)
        if ids_to_delete:
            backend_instance.delete(ids_to_delete)

    # Persist state
    new_state = PipelineState(
        last_release=release,
        last_run=datetime.now(timezone.utc).isoformat(),
        theme=theme,
        type=type_,
        bbox=area,
        backend=Backend(output_format),
        output=str(output) if output else db_url,
    )
    save_state(new_state, state_path)

    console.print(
        f"[green]✓ Done. Backend now has {backend_instance.count():,} features.[/green]"
    )


@update.command("status")
@click.option(
    "-f",
    "--format",
    "output_format",
    help="Output format (geojson, geojsonseq, geoparquet, postgis).",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Output file path (for file-based formats).",
)
@click.option("--db-url", help="Database URL (for postgis format).")
def update_status(output_format, output, db_url):
    """Show the current pipeline state.

    Requires either --output (for file formats) or --db-url (for postgis) to locate the state file.
    """
    from pathlib import Path
    from .state import load_state, get_state_file_for_backend
    from .models import Backend

    if not output and not db_url:
        click.echo(
            "Error: Must specify either --output or --db-url to check state.", err=True
        )
        sys.exit(1)

    if not output_format:
        # Try to infer format
        if db_url:
            output_format = "postgis"
        elif output:
            # Try to infer from extension
            ext = Path(output).suffix.lower()
            if ext in (".parquet", ".geoparquet"):
                output_format = "geoparquet"
            elif ext == ".geojson":
                output_format = "geojson"
            elif ext == ".geojsonl" or ext == ".jsonl":
                output_format = "geojsonseq"
            else:
                click.echo(
                    "Error: Cannot infer format from output file. Please specify --format.",
                    err=True,
                )
                sys.exit(1)
        else:
            click.echo("Error: Must specify --format when checking state.", err=True)
            sys.exit(1)

    backend = Backend(output_format)
    state_path = get_state_file_for_backend(backend, output, db_url)
    state = load_state(state_path)

    if state is None:
        click.echo(
            f"No state found at {state_path}. Run 'overturemaps download' to initialize.",
            err=True,
        )
        sys.exit(1)

    click.echo("Pipeline State:")
    click.echo(f"  Last release: {state.last_release}")
    click.echo(f"  Last run: {state.last_run}")
    click.echo(f"  Theme: {state.theme}")
    click.echo(f"  Type: {state.type}")
    b = state.bbox
    click.echo(f"  BBox: {b.xmin}, {b.ymin}, {b.xmax}, {b.ymax}")
    click.echo(f"  Backend: {state.backend.value}")
    click.echo(f"  Output: {state.output or '(none)'}")


def _resolve_backend(backend, output, db_url):
    """Instantiate the appropriate backend from CLI options."""
    from .backends import (
        GeoJSONBackend,
        GeoJSONSeqBackend,
        GeoParquetBackend,
        PostGISBackend,
    )
    from .models import Backend

    if backend == Backend.geojson:
        if output is None:
            click.echo("Error: --output is required for the geojson format.", err=True)
            sys.exit(1)
        return GeoJSONBackend(output)
    elif backend == Backend.geojsonseq:
        if output is None:
            click.echo(
                "Error: --output is required for the geojsonseq format.", err=True
            )
            sys.exit(1)
        return GeoJSONSeqBackend(output)
    elif backend == Backend.geoparquet:
        if output is None:
            click.echo(
                "Error: --output is required for the geoparquet format.", err=True
            )
            sys.exit(1)
        return GeoParquetBackend(output)
    elif backend == Backend.postgis:
        if db_url is None:
            click.echo("Error: --db-url is required for the postgis format.", err=True)
            sys.exit(1)
        table = "overture_features"
        return PostGISBackend(db_url, table)
    else:
        click.echo(f"Unknown format: {backend}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
