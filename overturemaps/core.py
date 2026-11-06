import io
import json
import sys
import threading
import warnings
from concurrent.futures import Future
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from ._http import MAX_RETRIES, open_url
from .models import BBox

STAC_CATALOG_URL = "https://stac.overturemaps.org/catalog.json"
STAC_INDEX_URL_TEMPLATE = "https://stac.overturemaps.org/{release}/collections.parquet"

# Cache for STAC catalog to avoid repeated network calls
_cached_stac_catalog = None

# Cache for each release's STAC file index. A published release's index doesn't
# change, so one download per release serves every bbox query in the process.
_cached_stac_indexes: dict[str, pa.Table] = {}
# Index downloads in progress, so concurrent queries for a release share one.
_stac_index_downloads: dict[str, Future] = {}
# Guards both dicts. Never held during a download.
_stac_index_lock = threading.Lock()


class StacIndexWarning(UserWarning):
    """The STAC file index couldn't be used, so a query is scanning every file of its type."""


# Allows for optional import of additional dependencies
try:
    from geopandas import GeoDataFrame

    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False

    class GeoDataFrame:
        pass


def _get_stac_catalog() -> dict:
    """
    Fetch and cache the STAC catalog.

    Returns
    -------
    dict: The STAC catalog JSON
    """
    global _cached_stac_catalog

    if _cached_stac_catalog is not None:
        return _cached_stac_catalog

    try:
        with open_url(STAC_CATALOG_URL, accept="application/json") as response:
            catalog = json.load(response)

        # Cache the catalog
        _cached_stac_catalog = catalog
        return catalog

    except Exception as e:
        raise Exception(f"Could not fetch STAC catalog: {e}") from e


def get_available_releases() -> Tuple[List[str], str]:
    """
    Fetch available releases from the STAC catalog.

    Returns
    -------
    Tuple of (all_releases, latest_release) where:
        - all_releases is a list of release version strings
        - latest_release is the latest release version string
    """
    catalog = _get_stac_catalog()

    latest_release = catalog.get("latest")

    # Extract release versions from the child links
    releases = []
    for link in catalog.get("links", []):
        if link.get("rel") == "child":
            href = link.get("href", "")
            # href may be relative ("./2025-09-24.0/catalog.json") or absolute
            # ("https://stac.overturemaps.org/2025-09-24.0/catalog.json").
            segments = [s for s in urlparse(href).path.split("/") if s and s != "."]
            release_version = segments[0] if segments else ""
            if release_version:
                releases.append(release_version)

    return releases, latest_release


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


def _coerce_bbox(
    bbox: BBox | tuple[float, float, float, float] | list[float] | None,
) -> BBox | None:
    """Normalize bbox input to a BBox instance."""
    if bbox is None:
        return None

    if isinstance(bbox, BBox):
        return bbox

    if len(bbox) != 4:
        raise ValueError("bbox must contain exactly 4 numeric values")

    return BBox(
        xmin=float(bbox[0]),
        ymin=float(bbox[1]),
        xmax=float(bbox[2]),
        ymax=float(bbox[3]),
    )


def _get_stac_index(release: str) -> pa.Table:
    """
    Fetch and cache the STAC-geoparquet index of a release's data files.

    Concurrent calls for a release share one download, and all get its exception
    if it fails. Failed downloads aren't cached, so a later call tries again.
    """
    stac_table = _cached_stac_indexes.get(release)
    if stac_table is not None:
        return stac_table

    with _stac_index_lock:
        stac_table = _cached_stac_indexes.get(release)
        if stac_table is not None:
            return stac_table
        download = _stac_index_downloads.get(release)
        is_downloader = download is None
        if is_downloader:
            download = _stac_index_downloads[release] = Future()

    if not is_downloader:
        return download.result()

    try:
        stac_table = _download_stac_index(release)
    except BaseException as e:
        # Hand over every exception, KeyboardInterrupt included, so waiters
        # never hang. Later calls start a new download.
        with _stac_index_lock:
            _stac_index_downloads.pop(release, None)
        download.set_exception(e)
        raise
    with _stac_index_lock:
        _cached_stac_indexes[release] = stac_table
        _stac_index_downloads.pop(release, None)
    download.set_result(stac_table)
    return stac_table


def _download_stac_index(release: str) -> pa.Table:
    stac_url = STAC_INDEX_URL_TEMPLATE.format(release=release)
    # Arrow can't read HTTP URLs directly; read into memory first
    with open_url(
        stac_url, accept="application/octet-stream", retries=MAX_RETRIES
    ) as response:
        data = response.read()
    return pq.read_table(io.BytesIO(data))


def _get_files_from_stac(
    theme: str, overture_type: str, bbox: BBox, release: str
) -> Optional[List[str]]:
    """
    Returns a list of bucket/key paths using the STAC-geoparquet index, or None
    after a StacIndexWarning if the index can't be read or lists no files of the type
    """
    try:
        stac_table = _get_stac_index(release)

        type_items = stac_table.filter(_stac_type_filter(overture_type))
        if type_items.num_rows == 0:
            # Handled like an unreadable index, so the query scans the type's
            # folder instead of reporting no data.
            raise ValueError(f"the index lists no {overture_type} files")

        xmin, ymin, xmax, ymax = bbox.as_tuple()
        bbox_filter = (
            (pc.field("bbox", "xmin") < xmax)
            & (pc.field("bbox", "xmax") > xmin)
            & (pc.field("bbox", "ymin") < ymax)
            & (pc.field("bbox", "ymax") > ymin)
        )

        table = type_items.filter(bbox_filter)

        if table.num_rows > 0:
            return _s3_paths(table)
        else:
            print(
                f"No data found for release {release} in query bbox {bbox.as_tuple()}.",
                file=sys.stderr,
            )
            return []

    except Exception as e:
        warnings.warn(
            f"Could not use the STAC index for release {release} ({e}). Falling back "
            f"to a full scan of s3://{_dataset_path(overture_type, release)}, which "
            "reads metadata from every file and can take several minutes. "
            "Results are not affected.",
            StacIndexWarning,
            stacklevel=_caller_stacklevel(),
        )
        return None


def _stac_type_filter(overture_type: str) -> pc.Expression:
    """Match the STAC index rows for the type's data files."""
    return (pc.field("collection") == overture_type) & (pc.field("type") == "Feature")


def _s3_paths(stac_items: pa.Table) -> List[str]:
    """Return the bucket/key paths of the data files in STAC index rows."""
    # clip out the "s3://" prefix
    return [
        assets["aws"]["alternate"]["s3"]["href"][len("s3://") :]
        for assets in stac_items.column("assets").to_pylist()
    ]


def _type_schema(
    overture_type: str, release: str, connect_timeout=None, request_timeout=None
) -> pa.Schema:
    """
    Read the type's schema from the first of its data files in the STAC index,
    which is quicker than listing the type's folder. Only called after
    _get_files_from_stac has found files of the type, so the index is cached.
    """
    stac_items = _get_stac_index(release).filter(_stac_type_filter(overture_type))
    dataset = ds.dataset(
        _s3_paths(stac_items.slice(0, 1)),
        filesystem=_s3_filesystem(connect_timeout, request_timeout),
    )
    return dataset.schema


def _caller_stacklevel() -> int:
    """Return the warnings stacklevel of the first caller outside this module."""
    frame = sys._getframe(1)
    level = 1
    while frame.f_back is not None and frame.f_globals.get("__name__") == __name__:
        frame = frame.f_back
        level += 1
    return level


def _s3_filesystem(connect_timeout=None, request_timeout=None) -> fs.S3FileSystem:
    return fs.S3FileSystem(
        anonymous=True,
        region="us-west-2",
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
    )


def _record_batch_reader_from_dataset(
    dataset: ds.Dataset,
    filter_expr=None,
) -> Optional[pa.RecordBatchReader]:
    """
    Create a RecordBatchReader from an S3 dataset with optional filtering.

    Parameters
    ----------
    dataset: pyarrow dataset
        Dataset to read from
    filter_expr: pyarrow expression, optional
        Filter to apply when reading the dataset

    Returns
    -------
    RecordBatchReader with the feature data, or None if error occurs
    """
    try:
        batches = dataset.to_batches(
            filter=filter_expr,
            use_threads=True,
            batch_readahead=16,
            fragment_readahead=4,
        )

        # Filter out empty batches to avoid downstream issues
        non_empty_batches = (b for b in batches if b.num_rows > 0)

        geoarrow_schema = geoarrow_schema_adapter(dataset.schema)
        return pa.RecordBatchReader.from_batches(geoarrow_schema, non_empty_batches)

    except Exception as e:
        print(f"Error reading dataset: {e}")
        return None


def _prepare_query(
    overture_type,
    bbox: BBox | tuple[float, float, float, float] | list[float] | None = None,
    release=None,
    connect_timeout=None,
    request_timeout=None,
    stac=True,
) -> Optional[Tuple[ds.Dataset, Optional[pc.Expression]]]:
    """
    Resolve the S3 dataset and filter expression for a given query.

    Returns the dataset and filter expression ready for counting or streaming,
    or None if STAC reports no files intersect the bbox. If the STAC index
    can't be read or lists no files of the type, warns with StacIndexWarning and
    scans every file of the type.
    """
    if release is None:
        release = get_latest_release()
    path = _dataset_path(overture_type, release)
    bbox_obj = _coerce_bbox(bbox)

    intersecting_files = None
    if bbox_obj and stac:
        intersecting_files = _get_files_from_stac(
            type_theme_map[overture_type], overture_type, bbox_obj, release
        )
        if intersecting_files is not None and len(intersecting_files) == 0:
            return None

    if bbox_obj:
        xmin, ymin, xmax, ymax = bbox_obj.as_tuple()
        filter_expr = (
            (pc.field("bbox", "xmin") < xmax)
            & (pc.field("bbox", "xmax") > xmin)
            & (pc.field("bbox", "ymin") < ymax)
            & (pc.field("bbox", "ymax") > ymin)
        )
    else:
        filter_expr = None

    dataset = ds.dataset(
        intersecting_files if intersecting_files is not None else path,
        filesystem=_s3_filesystem(connect_timeout, request_timeout),
    )

    return dataset, filter_expr


def count_rows(
    overture_type,
    bbox=None,
    release=None,
    connect_timeout=None,
    request_timeout=None,
    stac=True,
) -> int:
    """Return the number of rows matching the given parameters."""
    result = _prepare_query(
        overture_type, bbox, release, connect_timeout, request_timeout, stac
    )
    if result is None:
        return 0
    dataset, filter_expr = result
    return dataset.count_rows(filter=filter_expr)



def record_batch_reader(
    overture_type,
    bbox=None,
    release=None,
    connect_timeout=None,
    request_timeout=None,
    stac=True,
) -> Optional[pa.RecordBatchReader]:
    """Return a pyarrow RecordBatchReader for the desired bounding box and s3 path, or None on error."""
    if release is None:
        release = get_latest_release()
    result = _prepare_query(
        overture_type, bbox, release, connect_timeout, request_timeout, stac
    )
    if result is None:
        # No files intersect the bbox. Return an empty reader with the type's
        # schema, the same result a full scan gives.
        schema = _type_schema(overture_type, release, connect_timeout, request_timeout)
        return pa.RecordBatchReader.from_batches(geoarrow_schema_adapter(schema), [])
    dataset, filter_expr = result
    return _record_batch_reader_from_dataset(dataset, filter_expr=filter_expr)


def geodataframe(
    overture_type: str,
    bbox: BBox | tuple[float, float, float, float] | list[float] | None = None,
    release: str = None,
    connect_timeout: int = None,
    request_timeout: int = None,
    stac: bool = True,
) -> GeoDataFrame:
    """
    Loads geoparquet for specified type into a geopandas dataframe

    Parameters
    ----------
    overture_type: type to load
    bbox: optional bounding box for data fetch (xmin, ymin, xmax, ymax)
    release: optional release version, defaults to the latest
    connect_timeout: optional connection timeout in seconds
    request_timeout: optional request timeout in seconds
    stac: use the STAC index to read only the files that intersect the bbox
        (default True). If the index can't be read or lists no files of the
        type, a StacIndexWarning is issued and every file of the type is scanned.

    Returns
    -------
    GeoDataFrame with the optionally filtered theme data

    """
    if not HAS_GEOPANDAS:
        raise ImportError("geopandas is required to use this function")

    reader = record_batch_reader(
        overture_type,
        bbox=bbox,
        release=release,
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
        stac=stac,
    )
    return GeoDataFrame.from_arrow(reader)


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


# Registry manifest is now part of the STAC catalog
# Access via catalog.json -> registry property -> manifest field


def _binary_search_manifest(
    manifest_tuples: List[Tuple[str, str]], gers_id: str
) -> Optional[str]:
    """
    Binary search through manifest tuples to find the file containing the given GERS ID.

    Parameters
    ----------
    manifest_tuples: List of (filename, max_id) tuples, sorted by max_id
    gers_id: The GERS ID to search for (lowercase)

    Returns
    -------
    Filename containing the ID, or None if not found
    """
    left, right = 0, len(manifest_tuples) - 1

    while left <= right:
        mid = (left + right) // 2
        filename, max_id = manifest_tuples[mid]

        if gers_id <= max_id:
            # Check if this is the first file where max_id >= gers_id
            if mid == 0 or manifest_tuples[mid - 1][1] < gers_id:
                return filename
            else:
                # Search in the left half
                right = mid - 1
        else:
            # Search in the right half
            left = mid + 1

    return None


def query_gers_registry(gers_id: str) -> Optional[Tuple[str, BBox | None]]:
    """
    Query the GERS registry to get the filepath and bbox for a given GERS ID.

    The registry always uses the latest release.

    Parameters
    ----------
    gers_id: The GERS ID to look up

    Returns
    -------
    Tuple of (filepath, bbox) where bbox is a BBox, or None if not found
    """
    import sys

    release = get_latest_release()
    release_path = f"overturemaps-us-west-2/release/{release}"
    gers_id_lower = gers_id.lower()

    try:
        # Get the cached STAC catalog
        catalog = _get_stac_catalog()

        # Get the registry object from the catalog
        registry = catalog.get("registry")
        if registry is None:
            print("Registry configuration not found in STAC catalog", file=sys.stderr)
            return None

        # The registry contains 'path' and 'manifest'
        # manifest is a list of [filename, max_id] tuples
        registry_path = registry.get("path", "")
        manifest_tuples = registry.get("manifest", [])

        if not manifest_tuples:
            print("Registry manifest is empty in STAC catalog", file=sys.stderr)
            return None

        # Use binary search to find the file containing this GERS ID
        registry_file = _binary_search_manifest(manifest_tuples, gers_id_lower)

        if registry_file is None:
            print(f"{gers_id} does not exist in the GERS Registry.", file=sys.stderr)
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
            print(f"{gers_id} does not exist in the GERS Registry.", file=sys.stderr)
            return None

        # Get the first (should be only) result
        row = filtered_table.to_pylist()[0]
        path = row["path"]
        bbox_struct = row.get("bbox")
        version = row.get("version")
        first_seen = row.get("first_seen")
        last_seen = row.get("last_seen")
        last_changed = row.get("last_changed")

        # Check if path is NULL - means feature is not present in current release
        if path is None:
            print(
                f"GERS ID '{gers_id}' found in registry but not present in release {release}",
                file=sys.stderr,
            )
            print(f"  Version: {version}", file=sys.stderr)
            print(f"  First seen: {first_seen}", file=sys.stderr)
            print(f"  Last seen: {last_seen}", file=sys.stderr)
            if last_changed:
                print(f"  Last changed: {last_changed}", file=sys.stderr)
            return None

        # Construct full filepath
        if not path.startswith("/"):
            path = "/" + path
        filepath = f"{release_path}{path}"

        # Extract bbox values if available
        if bbox_struct is not None:
            bbox = BBox(
                xmin=bbox_struct["xmin"],
                ymin=bbox_struct["ymin"],
                xmax=bbox_struct["xmax"],
                ymax=bbox_struct["ymax"],
            )
        else:
            bbox = None

        # Write registry information to stderr
        print(f"Found GERS ID '{gers_id}' in release {release}", file=sys.stderr)
        print(f"  Version: {version}", file=sys.stderr)
        print(f"  Filepath: s3://{filepath}", file=sys.stderr)
        if bbox is not None:
            print(
                f"  Bbox: [{bbox.xmin:.6f}, {bbox.ymin:.6f}, {bbox.xmax:.6f}, {bbox.ymax:.6f}]",
                file=sys.stderr,
            )
        else:
            print(f"  Bbox: None", file=sys.stderr)
        print(f"  First seen: {first_seen}", file=sys.stderr)
        print(f"  Last seen: {last_seen}", file=sys.stderr)
        if last_changed:
            print(f"  Last changed: {last_changed}", file=sys.stderr)

        return (filepath, bbox)

    except Exception as e:
        print(f"Error querying GERS registry: {e}", file=sys.stderr)
        return None


def record_batch_reader_from_gers(
    gers_id: str,
    connect_timeout: int = None,
    request_timeout: int = None,
    registry_result: Optional[Tuple[str, BBox | None]] = None,
) -> Optional[pa.RecordBatchReader]:
    """
    Return a pyarrow RecordBatchReader for a specific GERS ID.

    The registry always uses the latest release.

    Parameters
    ----------
    gers_id: The GERS ID to look up
    connect_timeout: Optional connection timeout in seconds
    request_timeout: Optional request timeout in seconds
    registry_result: Optional pre-fetched registry result (filepath, bbox)
                    to avoid duplicate registry queries

    Returns
    -------
    RecordBatchReader with the feature data, or None if not found
    """
    # Use pre-fetched result if provided, otherwise query the registry
    if registry_result is None:
        result = query_gers_registry(gers_id)
        if result is None:
            return None
    else:
        result = registry_result

    filepath, bbox = result

    # Build filter expression based on ID and bbox (if available)
    filter_expr = pc.field("id") == gers_id.lower()

    if bbox is not None:
        xmin, ymin, xmax, ymax = bbox.as_tuple()
        bbox_filter = (
            (pc.field("bbox", "xmin") == xmin)
            & (pc.field("bbox", "ymin") == ymin)
            & (pc.field("bbox", "xmax") == xmax)
            & (pc.field("bbox", "ymax") == ymax)
        )
        filter_expr = filter_expr & bbox_filter

    try:
        dataset = ds.dataset(
            filepath,
            filesystem=fs.S3FileSystem(
                anonymous=True,
                region="us-west-2",
                connect_timeout=connect_timeout,
                request_timeout=request_timeout,
            ),
        )
    except Exception as e:
        print(f"Error opening dataset for GERS ID '{gers_id}': {e}", file=sys.stderr)
        return None
    return _record_batch_reader_from_dataset(dataset, filter_expr=filter_expr)
