"""
Writers and data pipeline for Overture Maps output formats.

Implements the context-manager writer protocol and the copy() pipeline
that streams RecordBatches from a reader to a writer.
"""

import json
import os
import sys

import orjson
import pyarrow.parquet as pq
import shapely
from tqdm import tqdm


def get_writer(output_format, path, schema):
    if output_format == "geojson":
        return GeoJSONWriter(path)
    elif output_format == "geojsonseq":
        return GeoJSONSeqWriter(path)
    elif output_format == "geoparquet":
        # Update the geoparquet metadata to remove the file-level bbox which
        # will no longer apply to this file. Since we cannot write the field at
        # the end, just remove it as it's optional. Let the per-row bounding
        # boxes do all the work.
        metadata = schema.metadata
        geo = json.loads(metadata[b"geo"])
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
        return pq.ParquetWriter(path, schema)


def copy(reader, writer):
    with tqdm(
        total=None, unit=" rows", desc="Downloading", file=sys.stderr, colour="blue"
    ) as bar:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            if batch.num_rows > 0:
                writer.write_batch(batch)
                bar.update(batch.num_rows)


class BaseGeoJSONWriter:
    """
    A base feature writer that manages either a file handle
    or output stream. Subclasses should implement write_feature()
    and finalize() if needed.
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

        geom_strings = shapely.to_geojson(
            shapely.from_wkb(batch.column("geometry").to_pylist())
        )

        prop_cols = [c for c in batch.schema.names if c not in ("geometry", "bbox")]
        rows = batch.select(prop_cols).to_pylist()

        for geom_str, row in zip(geom_strings, rows):
            self.write_feature(geom_str, row)

    def write_feature(self, geom_str, props):
        raise NotImplementedError(
            f"{self.__class__.__name__}.write_feature() must be implemented by subclasses"
        )

    def finalize(self):
        raise NotImplementedError(
            f"{self.__class__.__name__}.finalize() must be implemented by subclasses"
        )


class GeoJSONSeqWriter(BaseGeoJSONWriter):
    def write_feature(self, geom_str, props):
        props_str = orjson.dumps(
            {k: v for k, v in props.items() if v is not None}
        ).decode()
        self.writer.write(
            f'{{"type":"Feature","geometry":{geom_str},"properties":{props_str}}}\n'
        )


class GeoJSONWriter(BaseGeoJSONWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._has_written_feature = False
        self.writer.write('{"type": "FeatureCollection", "features": [\n')

    def write_feature(self, geom_str, props):
        props_str = orjson.dumps(
            {k: v for k, v in props.items() if v is not None}
        ).decode()
        if self._has_written_feature:
            self.writer.write(",\n")
        self.writer.write(
            f'{{"type":"Feature","geometry":{geom_str},"properties":{props_str}}}'
        )
        self._has_written_feature = True

    def finalize(self):
        self.writer.write("]}")
