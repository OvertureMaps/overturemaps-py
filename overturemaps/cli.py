"""
Overture Maps (overturemaps.org) command line utility.

Currently provides the ability to extract features from an Overture dataset in a
specified bounding box in a few different file formats.

"""

import json
import os
import sys
from typing import Optional

import click
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.fs as fs

from geoarrow.rust.core import write_geojson, write_geojson_lines, write_parquet

from .core import record_batch_reader, get_all_overture_types


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


@click.group()
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
def download(bbox, output_format, output, type_):
    if output is None:
        output = sys.stdout

    reader = record_batch_reader(type_, bbox)
    if reader is None:
        return

    if output_format == "geojson":
        write_geojson(reader, output)
    elif output_format == "geojsonseq":
        write_geojson_lines(reader, output)
    elif output_format == "geoparquet":
        write_parquet(reader, output)


if __name__ == "__main__":
    cli()
