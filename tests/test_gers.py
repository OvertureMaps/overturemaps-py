"""
Tests for GERS (Global Entity Reference System) functionality.
"""

import pyarrow as pa
import pytest
from overturemaps.core import query_gers_registry, record_batch_reader_from_gers


class TestGersIntegration:
    """Integration tests for GERS registry functionality."""

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
