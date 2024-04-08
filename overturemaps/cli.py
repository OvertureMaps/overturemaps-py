"""
Extract features from a Parquet dataset in a specified bounding box

With a parquet dataset that has the per-row bounding boxes, we can quickly
sub-select within a known region

"""

import itertools
import json
import os
import sys
from typing import Optional

import click
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq
import shapely.wkb


_theme_type_mapping = {
    "locality": "admins",
    "locality_area": "admins",
    "administrative_boundary": "admins",
    "building": "buildings",
    "part": "buildings",
    "place": "places",
    "segment": "transportation",
    "connector": "transportation",
    "land": "base",
    "land_use": "base",
    "water": "base",
}


def _dataset_path(overture_type: str) -> str:
    """
    Returns the s3 path of the Overture dataset to use. This assumes overture_type has
    been validated, e.g. by the CLI

    """
    theme = _theme_type_mapping[overture_type]
    return f"overturemaps-us-west-2/release/2024-03-12-alpha.0/theme={theme}/type={overture_type}/"


def record_batch_reader(path, bbox=None) -> Optional[pa.RecordBatchReader]:
    if bbox:
        xmin, ymin, xmax, ymax = bbox
        filter = (
            (pc.field("bbox", "minx") < xmax)
            & (pc.field("bbox", "maxx") > xmin)
            & (pc.field("bbox", "miny") < ymax)
            & (pc.field("bbox", "maxy") > ymin)
        )
    else:
        filter = None

    dataset = ds.dataset(
        path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
    )
    batches = dataset.to_batches(filter=filter)

    reader = pa.RecordBatchReader.from_batches(dataset.schema, batches)
    return reader


def get_writer(output_format, path, schema):
    if output_format == "geojson":
        writer = GeoJSONWriter(path)
    elif output_format == "geojsonseq":
        writer = GeoJSONSeqWriter(path)
    elif output_format == "parquet":
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
            self.fail(f"bbox must be 4 floating point numbers separated by commas. Got '{value}'")

        return bbox


@click.group()
def cli():
    pass


@cli.command()
@click.option("--bbox", required=False, type=BboxParamType())
@click.option("-f", "output_format", type=click.Choice(["geojson", "geojsonseq", "parquet"]), required=True)
@click.option("-o", "--output", required=False, type=click.Path())
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(list(_theme_type_mapping.keys())),
    required=True,
)
def download(bbox, output_format, output, type_):
    if output is None:
        output = sys.stdout

    path = _dataset_path(type_)
    reader = record_batch_reader(path, bbox)
    if reader is None:
        return

    with get_writer(output_format, output, schema=reader.schema) as writer:
        copy(reader, writer)


def copy(reader, writer, limit: int = None):
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        writer.write_batch(batch)


class BaseGeoJSONWriter:
    """
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


if __name__ == "__main__":
    cli()
