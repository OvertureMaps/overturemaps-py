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
import pyarrow.parquet as pq
import shapely.wkb
import itertools

from . core import record_batch_reader, get_all_overture_types
from . core import load_sources_from_path, type_theme_map


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
@click.option("-l", "--collect_licenses", is_flag=True)
def download(bbox, output_format, output, type_, collect_licenses):
    if output is None:
        output = sys.stdout

    reader = record_batch_reader(type_, bbox)
    if reader is None:
        return
    
    sources = None
    if collect_licenses:
        sources = SourceCollector("./overturemaps/sources", type_)

    with get_writer(output_format, output, schema=reader.schema) as writer:
        copy(reader, writer, sources)
    
    if collect_licenses:
        with open("LICENSES_COLLECTED_FROM_QUERY.json", "w") as f:
            f.write(sources.get_license_info())


def copy(reader, writer, sources):
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows > 0:
            if sources is not None:
                sources.collect_from_batch(batch)
            writer.write_batch(batch)


class SourceCollector:
    """
    extracts sources from Arrow batches and prints their licenses as JSON.
    """

    def __init__(self, sources_path, type_):
        self.theme = type_theme_map[type_]
        self.source_data = load_sources_from_path(sources_path)
        self.collected_source_names = set()

    def collect_from_batch(self, batch):
        # Dump the "sources" column into a list and flatten everything
        # Maybe there's some way to do this with Arrow?
        sources = batch.column("sources").to_pylist()
        flattened_sources = itertools.chain.from_iterable(sources)

        # This seems to work across all themes
        self.collected_source_names |= set([x["dataset"] for x in flattened_sources])
    
    def get_license_info(self):
        json_theme = self.source_data[self.theme][self.theme]
        source_data_dict = {} # holds all of the data for 
        for source in json_theme:
            source_data_dict[source["source_dataset_name"]] = source
        
        licenses = {}
        for source_name in self.collected_source_names:
            if source_name in source_data_dict:
                licenses[source_name] = source_data_dict[source_name]
            else:
                licenses[source_name] = "Could not find a license for this source; please check manually."
        
        return json.dumps(licenses, indent=4)
            

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


if __name__ == "__main__":
    cli()
