from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from urllib.request import urlopen
import io


# Keep the latest release at the bottom!
ALL_RELEASES = [
    '2025-08-20.0',
    '2025-08-20.1',
    '2025-09-24.0',
]

# Allows for optional import of additional dependencies
try:
    import geopandas as gpd
    from geopandas import GeoDataFrame

    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False
    GeoDataFrame = None


def _get_files_from_stac(theme: str, overture_type: str, bbox: tuple, release: str) -> Optional[List[str]]:
    """
    Returns a list of bucket/key paths using the STAC-geoparquet index
    """
    stac_url = f"https://stac.overturemaps.org/{release}/collections.parquet"
    try:
        # Arrow can't read HTTP URLs directly; read into memory first
        with urlopen(stac_url) as response:
            data = response.read()
            buffer = io.BytesIO(data)
            stac_table = pq.read_table(buffer)
        
        feature_type_filter = (pc.field("collection") == overture_type) \
                            & (pc.field("type") == "Feature")
        
        xmin, ymin, xmax, ymax = bbox
        bbox_filter = (
            (pc.field("bbox", "xmin") < xmax)
            & (pc.field("bbox", "xmax") > xmin)
            & (pc.field("bbox", "ymin") < ymax)
            & (pc.field("bbox", "ymax") > ymin)
        )
        
        combined_filter = feature_type_filter & bbox_filter
        table = stac_table.filter(combined_filter)
        
        if table.num_rows > 0:
            file_paths = table.column("assets").to_pylist()

            # clip out the "s3://" prefix
            s3_paths = [path["aws-s3"]["href"][len('s3://'):] for path in file_paths]
            return s3_paths
        else:
            print(f"No data found for release {release} in query bbox {bbox}.")
            return []
            
    except Exception as e:
        print(f"Error reading STAC index at {stac_url}: {e}")
        return None


def record_batch_reader(
    overture_type, bbox=None, release=None, connect_timeout=None, request_timeout=None, stac=False
) -> Optional[pa.RecordBatchReader]:
    """
    Return a pyarrow RecordBatchReader for the desired bounding box and s3 path
    """

    if release is None:
        release = ALL_RELEASES[-1]
    path = _dataset_path(overture_type, release)

    intersecting_files = None
    if bbox and stac:
        intersecting_files = _get_files_from_stac(type_theme_map[overture_type], overture_type, bbox, release)

    if bbox:
        xmin, ymin, xmax, ymax = bbox
        filter = (
            (pc.field("bbox", "xmin") < xmax)
            & (pc.field("bbox", "xmax") > xmin)
            & (pc.field("bbox", "ymin") < ymax)
            & (pc.field("bbox", "ymax") > ymin)
        )
    else:
        filter = None

    dataset = ds.dataset(
        intersecting_files if intersecting_files else path,
        filesystem=fs.S3FileSystem(
            anonymous=True,
            region="us-west-2",
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
        ),
    )
    batches = dataset.to_batches(filter=filter)

    # to_batches() can yield many batches with no rows. I've seen
    # this cause downstream crashes or other negative effects. For
    # example, the ParquetWriter will emit an empty row group for
    # each one bloating the size of a parquet file. Just omit
    # them so the RecordBatchReader only has non-empty ones. Use
    # the generator syntax so the batches are streamed out
    non_empty_batches = (b for b in batches if b.num_rows > 0)

    geoarrow_schema = geoarrow_schema_adapter(dataset.schema)
    reader = pa.RecordBatchReader.from_batches(geoarrow_schema, non_empty_batches)
    return reader


def geodataframe(
    overture_type: str,
    bbox: (float, float, float, float) = None,
    connect_timeout: int = None,
    request_timeout: int = None,
) -> GeoDataFrame:
    """
    Loads geoparquet for specified type into a geopandas dataframe

    Parameters
    ----------
    overture_type: type to load
    bbox: optional bounding box for data fetch (xmin, ymin, xmax, ymax)
    connect_timeout: optional connection timeout in seconds
    request_timeout: optional request timeout in seconds

    Returns
    -------
    GeoDataFrame with the optionally filtered theme data

    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required to use this function")

    reader = record_batch_reader(
        overture_type,
        bbox=bbox,
        connect_timeout=connect_timeout,
        request_timeout=request_timeout
    )
    return gpd.GeoDataFrame.from_arrow(reader)


def geoarrow_schema_adapter(schema: pa.Schema) -> pa.Schema:
    """
    Convert a geoarrow-compatible schema to a proper geoarrow schema

    This assumes there is a single "geometry" column with WKB formatting

    Parameters
    ----------
    schema: pa.Schema

    Returns
    -------
    pa.Schema
    A copy of the input schema with the geometry field replaced with
    a new one with the proper geoarrow ARROW:extension metadata

    """
    geometry_field_index = schema.get_field_index("geometry")
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)

    return geoarrow_schema


type_theme_map = {
    "address": "addresses",
    "bathymetry": "base",
    "building": "buildings",
    "building_part": "buildings",
    "division": "divisions",
    "division_area": "divisions",
    "division_boundary": "divisions",
    "place": "places",
    "segment": "transportation",
    "connector": "transportation",
    "infrastructure": "base",
    "land": "base",
    "land_cover": "base",
    "land_use": "base",
    "water": "base",
}


def _dataset_path(overture_type: str, release: str) -> str:
    """
    Returns the s3 path of the Overture dataset to use. This assumes overture_type has
    been validated, e.g. by the CLI

    """
    # Map of sub-partition "type" to parent partition "theme" for forming the
    # complete s3 path. Could be discovered by reading from the top-level s3
    # location but this allows to only read the files in the necessary partition.
    theme = type_theme_map[overture_type]
    return f"overturemaps-us-west-2/release/{release}/theme={theme}/type={overture_type}/"


def get_all_overture_types() -> List[str]:
    return list(type_theme_map.keys())

