"""Shared fixtures for STAC index tests that make no network requests."""

import io

import pyarrow as pa
import pytest

from overturemaps import core

from .fake_stac import stac_index_bytes


@pytest.fixture
def stac_index(mocker, monkeypatch):
    """Serve the fake STAC index through a mocked open_url, starting from an empty cache."""
    monkeypatch.setattr(core, "_cached_stac_indexes", {})
    data = stac_index_bytes()
    return mocker.patch(
        "overturemaps.core.open_url", side_effect=lambda *args, **kwargs: io.BytesIO(data)
    )


@pytest.fixture
def s3_dataset(mocker):
    """Replace pyarrow dataset discovery so no S3 request is made."""
    dataset = mocker.patch("overturemaps.core.ds.dataset")
    dataset.return_value.count_rows.return_value = 42
    dataset.return_value.schema = pa.schema(
        [pa.field("id", pa.string()), pa.field("geometry", pa.binary())]
    )
    return dataset
