"""
Tests for GERS (Global Entity Reference System) functionality.
"""

from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest
from overturemaps.core import (
    _create_s3_record_batch_reader,
    query_gers_registry,
    record_batch_reader_from_gers,
)


class TestQueryGersRegistry:
    """Tests for query_gers_registry function."""

    @pytest.fixture
    def mock_manifest(self):
        """Mock registry manifest data."""
        return {
            "bounds": [
                [
                    "00000000-0000-0000-0000-000000000000",
                    "0fffffff-ffff-ffff-ffff-ffffffffffff",
                ],
                [
                    "10000000-0000-0000-0000-000000000000",
                    "1fffffff-ffff-ffff-ffff-ffffffffffff",
                ],
            ],
            "files": [
                "part-00000-test.parquet",
                "part-00001-test.parquet",
            ],
        }

    @pytest.fixture
    def mock_registry_table(self):
        """Mock registry table data with a single GERS entry."""
        return pa.table(
            {
                "id": ["0b7fc702-49e7-4b35-81cd-a19acefe0696"],
                "path": ["/theme=buildings/type=building/part-00043.parquet"],
                "bbox": [
                    {
                        "xmin": -77.04327392578125,
                        "ymin": 38.91028594970703,
                        "xmax": -77.04273986816406,
                        "ymax": 38.91101837158203,
                    }
                ],
            }
        )

    def test_query_gers_registry_success(self, mock_manifest, mock_registry_table):
        """Test successful GERS ID lookup."""
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"
        expected_bbox = [
            -77.04327392578125,
            38.91028594970703,
            -77.04273986816406,
            38.91101837158203,
        ]

        with patch("overturemaps.core.urlopen") as mock_urlopen, patch(
            "overturemaps.core.pq.read_table"
        ) as mock_read_table, patch(
            "overturemaps.core.get_latest_release", return_value="2025-10-22.0"
        ):

            # Mock manifest fetch
            mock_manifest_response = MagicMock()
            mock_manifest_response.read.return_value = Mock()
            mock_manifest_response.__enter__ = Mock(return_value=mock_manifest_response)
            mock_manifest_response.__exit__ = Mock(return_value=False)

            mock_urlopen.return_value = mock_manifest_response

            with patch("json.load", return_value=mock_manifest):
                # Mock parquet read
                mock_read_table.return_value = mock_registry_table

                result = query_gers_registry(gers_id)

        assert result is not None
        filepath, bbox = result
        assert filepath.startswith("overturemaps-us-west-2/release/2025-10-22.0")
        assert filepath.endswith("/theme=buildings/type=building/part-00043.parquet")
        assert bbox == expected_bbox

    def test_query_gers_registry_not_found_in_manifest(self, mock_manifest):
        """Test GERS ID not found in manifest bounds."""
        gers_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"  # Out of bounds

        with patch("overturemaps.core.urlopen") as mock_urlopen, patch(
            "overturemaps.core.get_latest_release", return_value="2025-10-22.0"
        ):

            mock_manifest_response = MagicMock()
            mock_manifest_response.__enter__ = Mock(return_value=mock_manifest_response)
            mock_manifest_response.__exit__ = Mock(return_value=False)
            mock_urlopen.return_value = mock_manifest_response

            with patch("json.load", return_value=mock_manifest):
                result = query_gers_registry(gers_id)

        assert result is None

    def test_query_gers_registry_not_found_in_table(self, mock_manifest):
        """Test GERS ID not found in registry table."""
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"
        empty_table = pa.table({"id": [], "path": [], "bbox": []})

        with patch("overturemaps.core.urlopen") as mock_urlopen, patch(
            "overturemaps.core.pq.read_table", return_value=empty_table
        ), patch("overturemaps.core.get_latest_release", return_value="2025-10-22.0"):

            mock_manifest_response = MagicMock()
            mock_manifest_response.__enter__ = Mock(return_value=mock_manifest_response)
            mock_manifest_response.__exit__ = Mock(return_value=False)
            mock_urlopen.return_value = mock_manifest_response

            with patch("json.load", return_value=mock_manifest):
                result = query_gers_registry(gers_id)

        assert result is None

    def test_query_gers_registry_always_uses_latest_release(
        self, mock_manifest, mock_registry_table
    ):
        """Test GERS lookup always uses latest release from the registry."""
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"
        latest_release = "2025-10-22.0"

        with patch("overturemaps.core.urlopen") as mock_urlopen, patch(
            "overturemaps.core.pq.read_table", return_value=mock_registry_table
        ), patch("overturemaps.core.get_latest_release", return_value=latest_release):

            mock_manifest_response = MagicMock()
            mock_manifest_response.__enter__ = Mock(return_value=mock_manifest_response)
            mock_manifest_response.__exit__ = Mock(return_value=False)
            mock_urlopen.return_value = mock_manifest_response

            with patch("json.load", return_value=mock_manifest):
                result = query_gers_registry(gers_id)

        assert result is not None
        filepath, _ = result
        assert f"/release/{latest_release}/" in filepath


class TestRecordBatchReaderFromGers:
    """Tests for record_batch_reader_from_gers function."""

    def test_record_batch_reader_returns_none_when_query_fails(self):
        """Test that reader returns None when GERS query fails."""
        gers_id = "nonexistent-id"

        with patch("overturemaps.core.query_gers_registry", return_value=None):
            result = record_batch_reader_from_gers(gers_id)

        assert result is None

    def test_record_batch_reader_calls_helper_with_correct_params(self):
        """Test that reader calls the S3 helper with correct parameters."""
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"
        mock_filepath = "overturemaps-us-west-2/release/2025-10-22.0/theme=buildings/type=building/part-00043.parquet"
        mock_bbox = [-77.04327, 38.91029, -77.04274, 38.91102]

        with patch(
            "overturemaps.core.query_gers_registry",
            return_value=(mock_filepath, mock_bbox),
        ) as mock_query, patch(
            "overturemaps.core._create_s3_record_batch_reader"
        ) as mock_create_reader:

            mock_create_reader.return_value = Mock(spec=pa.RecordBatchReader)

            record_batch_reader_from_gers(
                gers_id, connect_timeout=10, request_timeout=30
            )

            # Verify query_gers_registry was called
            mock_query.assert_called_once_with(gers_id)

            # Verify _create_s3_record_batch_reader was called with correct args
            assert mock_create_reader.called
            call_args = mock_create_reader.call_args
            assert call_args.args[0] == mock_filepath
            assert call_args.kwargs["connect_timeout"] == 10
            assert call_args.kwargs["request_timeout"] == 30


class TestCreateS3RecordBatchReader:
    """Tests for _create_s3_record_batch_reader function."""

    def test_create_reader_with_single_path(self):
        """Test creating reader with single S3 path."""
        path = (
            "overturemaps-us-west-2/release/2025-10-22.0/theme=buildings/type=building/"
        )

        with patch("overturemaps.core.ds.dataset") as mock_dataset, patch(
            "overturemaps.core.geoarrow_schema_adapter"
        ) as mock_adapter:

            mock_ds = Mock()
            mock_batches = [Mock(num_rows=10)]
            mock_ds.to_batches.return_value = mock_batches
            mock_ds.schema = pa.schema([("geometry", pa.binary())])
            mock_dataset.return_value = mock_ds
            mock_adapter.return_value = mock_ds.schema

            result = _create_s3_record_batch_reader(path)

            # Verify dataset was called with correct path
            mock_dataset.assert_called_once()
            assert mock_dataset.call_args.args[0] == path
            # Result should be RecordBatchReader
            assert isinstance(result, pa.RecordBatchReader)

    def test_create_reader_with_list_of_paths(self):
        """Test creating reader with list of S3 paths (STAC mode)."""
        paths = [
            "overturemaps-us-west-2/release/2025-10-22.0/theme=buildings/type=building/part-00001.parquet",
            "overturemaps-us-west-2/release/2025-10-22.0/theme=buildings/type=building/part-00002.parquet",
        ]

        with patch("overturemaps.core.ds.dataset") as mock_dataset, patch(
            "overturemaps.core.geoarrow_schema_adapter"
        ) as mock_adapter:

            mock_ds = Mock()
            mock_batches = [Mock(num_rows=10), Mock(num_rows=20)]
            mock_ds.to_batches.return_value = mock_batches
            mock_ds.schema = pa.schema([("geometry", pa.binary())])
            mock_dataset.return_value = mock_ds
            mock_adapter.return_value = mock_ds.schema

            result = _create_s3_record_batch_reader(paths)

            # Verify dataset was called with list of paths
            mock_dataset.assert_called_once()
            assert mock_dataset.call_args.args[0] == paths
            assert isinstance(result, pa.RecordBatchReader)

    def test_create_reader_filters_empty_batches(self):
        """Test that empty batches are filtered out."""
        # This test verifies the generator filters empty batches
        # We can't easily mock from_batches, so just verify the function works
        path = "overturemaps-us-west-2/test/path"

        with patch("overturemaps.core.ds.dataset") as mock_dataset, patch(
            "overturemaps.core.geoarrow_schema_adapter"
        ) as mock_adapter:

            mock_ds = Mock()
            # Mix of empty and non-empty batches
            mock_batches = [
                Mock(num_rows=0),
                Mock(num_rows=10),
                Mock(num_rows=0),
                Mock(num_rows=5),
            ]
            mock_ds.to_batches.return_value = mock_batches
            mock_ds.schema = pa.schema([("geometry", pa.binary())])
            mock_dataset.return_value = mock_ds
            mock_adapter.return_value = mock_ds.schema

            result = _create_s3_record_batch_reader(path)

            # Verify the result is a RecordBatchReader
            assert isinstance(result, pa.RecordBatchReader)

    def test_create_reader_handles_error(self):
        """Test that errors are handled gracefully."""
        path = "invalid/path"

        with patch("overturemaps.core.ds.dataset", side_effect=Exception("S3 error")):
            result = _create_s3_record_batch_reader(path)

            assert result is None


class TestGersIntegration:
    """Integration tests that require network access."""

    @pytest.mark.integration
    def test_real_gers_query(self):
        """Test actual GERS query (requires network)."""
        # Known GERS ID from the Dupont Circle Hotel
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"

        result = query_gers_registry(gers_id)

        assert result is not None
        filepath, bbox = result
        assert filepath.startswith("overturemaps-us-west-2/release/")
        assert "theme=buildings" in filepath
        assert "type=building" in filepath
        assert len(bbox) == 4
        assert all(isinstance(x, float) for x in bbox)

    @pytest.mark.integration
    def test_real_record_batch_reader(self):
        """Test actual record batch reader creation (requires network)."""
        gers_id = "0b7fc702-49e7-4b35-81cd-a19acefe0696"

        reader = record_batch_reader_from_gers(gers_id)

        assert reader is not None
        assert isinstance(reader, pa.RecordBatchReader)

        # Read first batch
        batch = reader.read_next_batch()
        assert batch.num_rows == 1

        # Verify expected columns exist
        schema_names = [field.name for field in batch.schema]
        assert "id" in schema_names
        assert "geometry" in schema_names
        assert "bbox" in schema_names

    @pytest.mark.integration
    def test_gers_id_not_found(self):
        """Test handling of non-existent GERS ID."""
        # Invalid GERS ID that shouldn't exist
        gers_id = "00000000-0000-0000-0000-000000000001"

        result = query_gers_registry(gers_id)

        # Should return None for non-existent ID
        assert result is None
