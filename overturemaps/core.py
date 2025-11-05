import io
import json
from typing import List, Optional, Tuple
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

STAC_CATALOG_URL = "https://stac.overturemaps.org/catalog.json"

# Cache for releases to avoid repeated network calls
_cached_releases = None
_cached_latest_release = None


def get_available_releases() -> Tuple[List[str], str]:
    """
    Fetch available releases from the STAC catalog.

    Returns
    -------
    Tuple of (all_releases, latest_release) where:
        - all_releases is a list of release version strings
        - latest_release is the latest release version string
    """
    global _cached_releases, _cached_latest_release

    if _cached_releases is not None and _cached_latest_release is not None:
        return _cached_releases, _cached_latest_release

    try:
        with urlopen(STAC_CATALOG_URL) as response:
            catalog = json.load(response)

        latest_release = catalog.get("latest")

        # Extract release versions from the child links
        releases = []
        for link in catalog.get("links", []):
            if link.get("rel") == "child":
                href = link.get("href", "")
                # href format is "./2025-09-24.0/catalog.json"
                release_version = href.strip("./").split("/")[0]
                if release_version:
                    releases.append(release_version)

        # Cache the results
        _cached_releases = releases
        _cached_latest_release = latest_release

        return releases, latest_release

    except Exception as e:
        print(f"Warning: Could not fetch releases from STAC catalog: {e}")
        print("Falling back to hardcoded releases")
        # Fallback to hardcoded releases
        fallback_releases = [
            "2025-09-24.0",
            "2025-10-22.0",
        ]
        return fallback_releases, fallback_releases[-1]


def get_latest_release() -> str:
    """
    Get the latest release version.

    Returns
    -------
    str: The latest release version
    """
    _, latest = get_available_releases()
    return latest


# For backwards compatibility, expose ALL_RELEASES as a list
# This will be populated dynamically when first accessed
def _get_all_releases():
    releases, _ = get_available_releases()
    return releases


# Lazy evaluation property-like access
class _ReleasesProxy:
    def __getitem__(self, index):
        return _get_all_releases()[index]

    def __iter__(self):
        return iter(_get_all_releases())

    def __len__(self):
        return len(_get_all_releases())

    def __repr__(self):
        return repr(_get_all_releases())


ALL_RELEASES = _ReleasesProxy()

# Allows for optional import of additional dependencies
try:
    import geopandas as gpd
    from geopandas import GeoDataFrame

    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False
    GeoDataFrame = None


def _get_files_from_stac(
    theme: str, overture_type: str, bbox: tuple, release: str
) -> Optional[List[str]]:
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

        feature_type_filter = (pc.field("collection") == overture_type) & (
            pc.field("type") == "Feature"
        )

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
            s3_paths = [path["aws-s3"]["href"][len("s3://") :] for path in file_paths]
            return s3_paths
        else:
            print(f"No data found for release {release} in query bbox {bbox}.")
            return []

    except Exception as e:
        print(f"Error reading STAC index at {stac_url}: {e}")
        return None


def _create_s3_record_batch_reader(
    path,
    filter_expr=None,
    connect_timeout=None,
    request_timeout=None,
) -> Optional[pa.RecordBatchReader]:
    """
    Create a RecordBatchReader from S3 path(s) with optional filtering.

    Parameters
    ----------
    path: str or list of str
        S3 path(s) in the format "bucket/key" (without s3:// prefix)
    filter_expr: pyarrow expression, optional
        Filter to apply when reading the dataset
    connect_timeout: int, optional
        Connection timeout in seconds
    request_timeout: int, optional
        Request timeout in seconds

    Returns
    -------
    RecordBatchReader with the feature data, or None if error occurs
    """
    try:
        dataset = ds.dataset(
            path,
            filesystem=fs.S3FileSystem(
                anonymous=True,
                region="us-west-2",
                connect_timeout=connect_timeout,
                request_timeout=request_timeout,
            ),
        )

        batches = dataset.to_batches(filter=filter_expr)

        # Filter out empty batches to avoid downstream issues
        non_empty_batches = (b for b in batches if b.num_rows > 0)

        geoarrow_schema = geoarrow_schema_adapter(dataset.schema)
        reader = pa.RecordBatchReader.from_batches(geoarrow_schema, non_empty_batches)

        return reader

    except Exception as e:
        print(f"Error reading data from path {path}: {e}")
        return None


def record_batch_reader(
    overture_type,
    bbox=None,
    release=None,
    connect_timeout=None,
    request_timeout=None,
    stac=False,
) -> Optional[pa.RecordBatchReader]:
    """
    Return a pyarrow RecordBatchReader for the desired bounding box and s3 path
    """

    if release is None:
        release = get_latest_release()
    path = _dataset_path(overture_type, release)

    intersecting_files = None
    if bbox and stac:
        intersecting_files = _get_files_from_stac(
            type_theme_map[overture_type], overture_type, bbox, release
        )

    if bbox:
        xmin, ymin, xmax, ymax = bbox
        filter_expr = (
            (pc.field("bbox", "xmin") < xmax)
            & (pc.field("bbox", "xmax") > xmin)
            & (pc.field("bbox", "ymin") < ymax)
            & (pc.field("bbox", "ymax") > ymin)
        )
    else:
        filter_expr = None

    return _create_s3_record_batch_reader(
        intersecting_files if intersecting_files else path,
        filter_expr=filter_expr,
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
    )


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

    reader = record_batch_reader(overture_type, bbox, connect_timeout, request_timeout)
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
    return (
        f"overturemaps-us-west-2/release/{release}/theme={theme}/type={overture_type}/"
    )


def get_all_overture_types() -> List[str]:
    return list(type_theme_map.keys())


REGISTRY_MANIFEST_URL = "https://labs.overturemaps.org/data/registry-manifest.json"


def query_gers_registry(
    gers_id: str, release: str = None
) -> Optional[Tuple[str, List[float]]]:
    """
    Query the GERS registry to get the filepath and bbox for a given GERS ID.

    Parameters
    ----------
    gers_id: The GERS ID to look up
    release: Optional release version (defaults to latest)

    Returns
    -------
    Tuple of (filepath, bbox) where bbox is [xmin, ymin, xmax, ymax], or None if not found
    """
    if release is None:
        release = get_latest_release()

    release_path = f"overturemaps-us-west-2/release/{release}"
    gers_id_lower = gers_id.lower()

    try:
        # Query the registry manifest to find which file contains this ID
        with urlopen(REGISTRY_MANIFEST_URL) as response:
            manifest = json.load(response)

        # The manifest has "bounds" (list of [min_id, max_id] pairs) and "files"
        bounds = manifest.get("bounds", [])
        files = manifest.get("files", [])

        # Find the registry file that contains this GERS ID
        registry_file = None
        for i, (min_id, max_id) in enumerate(bounds):
            if min_id <= gers_id_lower <= max_id:
                registry_file = files[i]
                break

        if registry_file is None:
            print(f"GERS ID '{gers_id}' not found in registry manifest")
            return None

        # Read the specific registry file with filter (predicate pushdown)
        # This only reads the relevant row groups instead of the entire file
        registry_path = f"overturemaps-us-west-2/registry/{registry_file}"
        filesystem = fs.S3FileSystem(anonymous=True, region="us-west-2")

        # Use filters parameter for predicate pushdown
        filtered_table = pq.read_table(
            registry_path, filesystem=filesystem, filters=[("id", "=", gers_id_lower)]
        )

        if filtered_table.num_rows == 0:
            print(f"GERS ID '{gers_id}' not found in registry file {registry_file}")
            return None

        # Get the first (should be only) result
        row = filtered_table.to_pylist()[0]
        path = row["path"]
        bbox_struct = row["bbox"]

        # Construct full filepath
        if not path.startswith("/"):
            path = "/" + path
        filepath = f"{release_path}{path}"

        # Extract bbox values
        bbox = [
            bbox_struct["xmin"],
            bbox_struct["ymin"],
            bbox_struct["xmax"],
            bbox_struct["ymax"],
        ]

        return (filepath, bbox)

    except Exception as e:
        print(f"Error querying GERS registry: {e}")
        return None


def record_batch_reader_from_gers(
    gers_id: str,
    release: str = None,
    connect_timeout: int = None,
    request_timeout: int = None,
) -> Optional[pa.RecordBatchReader]:
    """
    Return a pyarrow RecordBatchReader for a specific GERS ID by querying the registry.

    Parameters
    ----------
    gers_id: The GERS ID to look up
    release: Optional release version (defaults to latest)
    connect_timeout: Optional connection timeout in seconds
    request_timeout: Optional request timeout in seconds

    Returns
    -------
    RecordBatchReader with the feature data, or None if not found
    """
    result = query_gers_registry(gers_id, release)

    if result is None:
        return None

    filepath, bbox = result

    xmin, ymin, xmax, ymax = bbox
    filter_expr = (
        (pc.field("id") == gers_id.lower())
        & (pc.field("bbox", "xmin") == xmin)
        & (pc.field("bbox", "ymin") == ymin)
        & (pc.field("bbox", "xmax") == xmax)
        & (pc.field("bbox", "ymax") == ymax)
    )

    return _create_s3_record_batch_reader(
        filepath,
        filter_expr=filter_expr,
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
    )
