"""Tests for the copy() function and its background count_rows thread."""

import threading
import time

import pyarrow as pa
import pytest

from overturemaps.cli import copy


class _FakeBatch:
    def __init__(self, num_rows):
        self.num_rows = num_rows


class _FakeReader:
    """Yields batches with an optional per-batch delay."""

    def __init__(self, batches, delay=0.0):
        self._batches = iter(batches)
        self._delay = delay

    def read_next_batch(self):
        if self._delay:
            time.sleep(self._delay)
        try:
            return next(self._batches)
        except StopIteration:
            raise StopIteration


class _FakeWriter:
    def __init__(self):
        self.batches = []

    def write_batch(self, batch):
        self.batches.append(batch)


def _capture_bar(monkeypatch):
    """Patch tqdm so we can inspect the bar instance after copy() returns."""
    import overturemaps.cli as cli_module

    bars = []
    real_tqdm = cli_module.tqdm

    class CapturingTqdm(real_tqdm):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            bars.append(self)

    monkeypatch.setattr(cli_module, "tqdm", CapturingTqdm)
    return bars


def test_copy_sets_total_when_count_resolves_first(monkeypatch):
    """bar.total is updated when count_rows_fn resolves before the download ends."""
    bars = _capture_bar(monkeypatch)

    # Slow reader: each batch takes 50 ms → 3 batches = ~150 ms total
    reader = _FakeReader([_FakeBatch(10)] * 3, delay=0.05)
    writer = _FakeWriter()

    # Fast count: resolves almost immediately
    def fast_count():
        return 30

    copy(reader, writer, count_rows_fn=fast_count)

    assert bars[0].total == 30


def test_copy_total_stays_none_when_download_finishes_first(monkeypatch):
    """bar.total stays None when the download finishes before count_rows_fn resolves."""
    bars = _capture_bar(monkeypatch)

    # Fast reader: no delay
    reader = _FakeReader([_FakeBatch(10)] * 3)
    writer = _FakeWriter()

    resolved = threading.Event()

    # Slow count: takes 200 ms — download will be done by then
    def slow_count():
        time.sleep(0.2)
        resolved.set()
        return 30

    copy(reader, writer, count_rows_fn=slow_count)

    # Give the background thread time to finish so we're not racing
    resolved.wait(timeout=1.0)
    assert bars[0].total is None


def test_copy_survives_count_rows_fn_exception(monkeypatch):
    """Download completes normally even if count_rows_fn raises."""
    bars = _capture_bar(monkeypatch)

    reader = _FakeReader([_FakeBatch(5)] * 2)
    writer = _FakeWriter()

    def broken_count():
        raise RuntimeError("S3 unavailable")

    copy(reader, writer, count_rows_fn=broken_count)

    assert len(writer.batches) == 2
    assert bars[0].total is None


def test_copy_without_count_rows_fn(monkeypatch):
    """copy() works fine with no count_rows_fn — no thread is started."""
    bars = _capture_bar(monkeypatch)

    reader = _FakeReader([_FakeBatch(7)])
    writer = _FakeWriter()

    copy(reader, writer)

    assert len(writer.batches) == 1
    assert bars[0].total is None
