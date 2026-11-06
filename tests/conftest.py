"""Shared fixtures for STAC index tests that make no network requests."""

import io
import threading
from concurrent.futures import Future

import pyarrow as pa
import pytest

from overturemaps import core

from .fake_stac import stac_index_bytes


@pytest.fixture
def empty_stac_cache(monkeypatch):
    """Start with no cached or in-progress STAC index downloads."""
    monkeypatch.setattr(core, "_cached_stac_indexes", {})
    monkeypatch.setattr(core, "_stac_index_downloads", {})


@pytest.fixture
def stac_index(mocker, empty_stac_cache):
    """Serve the fake STAC index through a mocked open_url, starting from an empty cache."""
    data = stac_index_bytes()
    return mocker.patch(
        "overturemaps.core.open_url", side_effect=lambda *args, **kwargs: io.BytesIO(data)
    )


@pytest.fixture
def index_waiters(mocker):
    """A semaphore released each time a caller waits on another caller's index download."""
    waiting = threading.Semaphore(0)

    class WatchedDownload(Future):
        def result(self, timeout=None):
            waiting.release()
            return super().result(timeout)

    mocker.patch("overturemaps.core.Future", WatchedDownload)
    return waiting


@pytest.fixture
def s3_dataset(mocker):
    """Replace pyarrow dataset discovery so no S3 request is made."""
    dataset = mocker.patch("overturemaps.core.ds.dataset")
    dataset.return_value.count_rows.return_value = 42
    dataset.return_value.schema = pa.schema(
        [pa.field("id", pa.string()), pa.field("geometry", pa.binary())]
    )
    return dataset
