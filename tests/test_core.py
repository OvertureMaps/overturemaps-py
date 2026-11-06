"""Tests for pure (no-network) functions in core.py."""

import inspect
import io
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError

import pyarrow as pa
import pytest

from overturemaps import _http, core
from overturemaps.core import (
    StacIndexWarning,
    _binary_search_manifest,
    _coerce_bbox,
    _dataset_path,
    _get_files_from_stac,
    _get_stac_index,
    count_rows,
    geoarrow_schema_adapter,
    geodataframe,
    get_all_overture_types,
    get_available_releases,
    record_batch_reader,
    type_theme_map,
)
from overturemaps.models import BBox

from .fake_stac import OCEAN_BBOX, RELEASE, SF_BBOX, WEST_FILE, stac_index_bytes


class TestCoerceBbox:
    def test_none_returns_none(self):
        assert _coerce_bbox(None) is None

    def test_bbox_instance_passthrough(self):
        bbox = BBox(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0)
        assert _coerce_bbox(bbox) is bbox

    def test_from_tuple(self):
        result = _coerce_bbox((-71.1, 42.3, -71.0, 42.4))
        assert isinstance(result, BBox)
        assert result.xmin == -71.1
        assert result.ymin == 42.3
        assert result.xmax == -71.0
        assert result.ymax == 42.4

    def test_from_list(self):
        result = _coerce_bbox([-71.1, 42.3, -71.0, 42.4])
        assert isinstance(result, BBox)
        assert result.ymin == 42.3

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError):
            _coerce_bbox((1.0, 2.0, 3.0))


class TestDatasetPath:
    def test_building_contains_theme_and_type(self):
        path = _dataset_path("building", "2025-01-22.0")
        assert "buildings" in path
        assert "building" in path
        assert "2025-01-22.0" in path

    def test_path_starts_with_s3_bucket(self):
        path = _dataset_path("place", "2025-01-22.0")
        assert path.startswith("overturemaps-us-west-2/release/")
        assert "places" in path
        assert "place" in path


class TestBinarySearchManifest:
    def test_first_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "aaa") == "file1.parquet"

    def test_middle_file(self):
        manifest = [
            ("file1.parquet", "bbb"),
            ("file2.parquet", "ddd"),
            ("file3.parquet", "fff"),
        ]
        assert _binary_search_manifest(manifest, "ccc") == "file2.parquet"

    def test_exact_max_id_matches_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "bbb") == "file1.parquet"

    def test_last_file(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "ddd") == "file2.parquet"

    def test_not_found_returns_none(self):
        manifest = [("file1.parquet", "bbb"), ("file2.parquet", "ddd")]
        assert _binary_search_manifest(manifest, "zzz") is None

    def test_empty_manifest_returns_none(self):
        assert _binary_search_manifest([], "aaa") is None


class TestGetAllOvertureTypes:
    def test_returns_nonempty_list(self):
        result = get_all_overture_types()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_includes_core_types(self):
        types = get_all_overture_types()
        for t in ("building", "place", "segment", "water", "land"):
            assert t in types

    def test_matches_type_theme_map_keys(self):
        assert set(get_all_overture_types()) == set(type_theme_map.keys())


class TestGeoarrowSchemaAdapter:
    def test_adds_extension_metadata_to_geometry(self):
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("geometry", pa.large_binary())]
        )
        result = geoarrow_schema_adapter(schema)
        geom_field = result.field("geometry")
        assert geom_field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"

    def test_non_geometry_fields_unchanged(self):
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("geometry", pa.large_binary())]
        )
        result = geoarrow_schema_adapter(schema)
        assert result.field("id").type == pa.string()
        assert result.field("id").metadata is None


class TestGetAvailableReleases:
    """Regression tests for parsing STAC catalog child link hrefs.

    A prior implementation used ``href.strip("./").split("/")[0]``, which
    assumed a relative href like ``./2025-09-24.0/catalog.json``. The live
    STAC catalog started returning absolute hrefs, which that strip/split
    silently mis-parsed into ``"https:"`` instead of the release ID. These
    mock the catalog so the case is verified without depending on remote
    catalog contents.
    """

    def test_relative_child_hrefs(self, monkeypatch):
        catalog = {
            "latest": "2025-09-24.0",
            "links": [
                {"rel": "child", "href": "./2025-09-24.0/catalog.json"},
                {"rel": "child", "href": "./2025-08-20.0/catalog.json"},
                {"rel": "self", "href": "./catalog.json"},
            ],
        }
        monkeypatch.setattr("overturemaps.core._cached_stac_catalog", catalog)
        releases, latest = get_available_releases()
        assert sorted(releases) == ["2025-08-20.0", "2025-09-24.0"]
        assert latest == "2025-09-24.0"

    def test_absolute_child_hrefs(self, monkeypatch):
        catalog = {
            "latest": "2026-08-19.0",
            "links": [
                {
                    "rel": "child",
                    "href": "https://stac.overturemaps.org/2026-08-19.0/catalog.json",
                },
                {
                    "rel": "child",
                    "href": "https://stac.overturemaps.org/2026-07-22.0/catalog.json",
                },
                {"rel": "self", "href": "https://stac.overturemaps.org/catalog.json"},
            ],
        }
        monkeypatch.setattr("overturemaps.core._cached_stac_catalog", catalog)
        releases, latest = get_available_releases()
        assert sorted(releases) == ["2026-07-22.0", "2026-08-19.0"]
        assert latest == "2026-08-19.0"


class TestStacIndex:
    def test_returns_only_files_that_intersect_bbox(self, stac_index):
        files = _get_files_from_stac("buildings", "building", BBox(*SF_BBOX), RELEASE)
        assert files == [WEST_FILE]

    def test_returns_empty_list_when_nothing_intersects(self, stac_index):
        assert _get_files_from_stac("buildings", "building", BBox(*OCEAN_BBOX), RELEASE) == []

    def test_index_is_downloaded_once_per_release(self, stac_index):
        _get_files_from_stac("buildings", "building", BBox(*SF_BBOX), RELEASE)
        _get_files_from_stac("buildings", "building", BBox(*OCEAN_BBOX), RELEASE)
        _get_files_from_stac("places", "place", BBox(*SF_BBOX), RELEASE)
        assert stac_index.call_count == 1

        _get_files_from_stac("buildings", "building", BBox(*SF_BBOX), "2026-09-23.0")
        assert stac_index.call_count == 2
        assert stac_index.call_args.args[0] == (
            "https://stac.overturemaps.org/2026-09-23.0/collections.parquet"
        )

    def test_concurrent_queries_download_index_once(self, stac_index):
        data = stac_index_bytes()

        def slow_open_url(*args, **kwargs):
            time.sleep(0.05)
            return io.BytesIO(data)

        stac_index.side_effect = slow_open_url
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda _: _get_files_from_stac(
                        "buildings", "building", BBox(*SF_BBOX), RELEASE
                    ),
                    range(16),
                )
            )

        assert results == [[WEST_FILE]] * 16
        assert stac_index.call_count == 1

    def test_failed_download_is_not_cached(self, stac_index):
        stac_index.side_effect = [URLError("simulated outage"), io.BytesIO(stac_index_bytes())]

        with pytest.raises(URLError):
            _get_stac_index(RELEASE)
        assert _get_stac_index(RELEASE).num_rows == 3
        assert stac_index.call_count == 2


class TestStacByDefault:
    @pytest.mark.parametrize("function", [count_rows, record_batch_reader, geodataframe])
    def test_query_functions_use_stac_by_default(self, function):
        assert inspect.signature(function).parameters["stac"].default is True

    def test_bbox_query_reads_only_intersecting_files(self, stac_index, s3_dataset):
        assert count_rows("building", bbox=SF_BBOX, release=RELEASE) == 42
        assert s3_dataset.call_args.args[0] == [WEST_FILE]

    def test_query_without_bbox_skips_index(self, stac_index, s3_dataset):
        count_rows("building", release=RELEASE)
        stac_index.assert_not_called()
        assert s3_dataset.call_args.args[0] == _dataset_path("building", RELEASE)

    def test_stac_false_skips_index(self, stac_index, s3_dataset):
        count_rows("building", bbox=SF_BBOX, release=RELEASE, stac=False)
        stac_index.assert_not_called()
        assert s3_dataset.call_args.args[0] == _dataset_path("building", RELEASE)


class TestStacFallback:
    @pytest.mark.parametrize("function", [count_rows, record_batch_reader, geodataframe])
    def test_unreadable_index_warns_and_scans_every_file(self, stac_index, s3_dataset, function):
        if function is geodataframe:
            pytest.importorskip("geopandas")
            s3_dataset.return_value.to_batches.return_value = []
        stac_index.side_effect = URLError("simulated outage")

        with pytest.warns(StacIndexWarning, match="full scan") as caught:
            function("building", bbox=SF_BBOX, release=RELEASE)

        assert s3_dataset.call_args.args[0] == _dataset_path("building", RELEASE)
        message = str(caught[0].message)
        assert RELEASE in message
        assert "simulated outage" in message
        # The warning points at the caller, not at overturemaps internals.
        assert caught[0].filename == __file__

    def test_warning_filter_can_turn_fallback_into_an_error(self, stac_index, s3_dataset):
        stac_index.side_effect = URLError("simulated outage")

        with warnings.catch_warnings():
            warnings.simplefilter("error", StacIndexWarning)
            with pytest.raises(StacIndexWarning):
                count_rows("building", bbox=SF_BBOX, release=RELEASE)

        s3_dataset.assert_not_called()

    def test_rate_limited_index_is_retried_before_falling_back(
        self, mocker, monkeypatch, s3_dataset
    ):
        monkeypatch.setattr(core, "_cached_stac_indexes", {})
        sleep = mocker.patch("overturemaps._http.time.sleep")
        urlopen = mocker.patch(
            "overturemaps._http.urlopen",
            side_effect=HTTPError("url", 429, "Too Many Requests", {}, None),
        )

        with pytest.warns(StacIndexWarning, match="HTTP Error 429"):
            count_rows("building", bbox=SF_BBOX, release=RELEASE)

        assert urlopen.call_count == _http.MAX_RETRIES + 1
        assert sleep.call_count == _http.MAX_RETRIES
        assert s3_dataset.call_args.args[0] == _dataset_path("building", RELEASE)


class TestEmptyArea:
    def test_count_rows_is_zero_without_reading_s3(self, stac_index, s3_dataset):
        assert count_rows("building", bbox=OCEAN_BBOX, release=RELEASE) == 0
        s3_dataset.assert_not_called()

    def test_record_batch_reader_is_empty_with_type_schema(self, stac_index, s3_dataset):
        reader = record_batch_reader("building", bbox=OCEAN_BBOX, release=RELEASE)

        assert reader is not None
        assert reader.schema.names == ["id", "geometry"]
        assert reader.read_all().num_rows == 0
        # Only the type's schema is read, from the full type path.
        assert s3_dataset.call_args.args[0] == _dataset_path("building", RELEASE)

    def test_geodataframe_is_empty_with_type_columns(self, stac_index, s3_dataset):
        pytest.importorskip("geopandas")

        gdf = geodataframe("building", bbox=OCEAN_BBOX, release=RELEASE)

        assert len(gdf) == 0
        assert list(gdf.columns) == ["id", "geometry"]
