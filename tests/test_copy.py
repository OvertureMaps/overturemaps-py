"""Tests for the copy() function."""

from overturemaps.cli import copy


class _FakeBatch:
    def __init__(self, num_rows):
        self.num_rows = num_rows


class _FakeReader:
    def __init__(self, batches):
        self._batches = iter(batches)

    def read_next_batch(self):
        try:
            return next(self._batches)
        except StopIteration:
            raise StopIteration


class _FakeWriter:
    def __init__(self):
        self.batches = []

    def write_batch(self, batch):
        self.batches.append(batch)


def test_copy_writes_all_batches():
    reader = _FakeReader([_FakeBatch(10), _FakeBatch(20), _FakeBatch(5)])
    writer = _FakeWriter()

    copy(reader, writer)

    assert len(writer.batches) == 3


def test_copy_skips_empty_batches():
    reader = _FakeReader([_FakeBatch(0), _FakeBatch(10), _FakeBatch(0)])
    writer = _FakeWriter()

    copy(reader, writer)

    assert len(writer.batches) == 1


def test_copy_empty_reader():
    reader = _FakeReader([])
    writer = _FakeWriter()

    copy(reader, writer)

    assert len(writer.batches) == 0
