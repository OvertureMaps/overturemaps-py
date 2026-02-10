"""Tests for overture_toolkit.changelog module."""

from __future__ import annotations

import warnings

from overturemaps.changelog import classify_changes
from overturemaps.models import BBox, ChangeRecord, ChangeType


def _make_records(raw: list[dict]) -> list[ChangeRecord]:
    """Convert raw dicts (from conftest) into ChangeRecord objects."""
    records = []
    for d in raw:
        bbox = None
        if d.get("bbox"):
            b = d["bbox"]
            bbox = BBox(b["xmin"], b["ymin"], b["xmax"], b["ymax"])
        records.append(
            ChangeRecord(
                id=d["id"],
                change_type=ChangeType(d["change_type"]),
                successor_ids=d.get("successor_ids", []),
                bbox=bbox,
            )
        )
    return records


def test_classify_changes_splits_correctly(sample_changelog_records):
    """classify_changes returns correct added/modified/deleted sets."""
    records = _make_records(sample_changelog_records)
    # Suppress deprecation warning for this test
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ids_to_add, ids_to_modify, ids_to_delete = classify_changes(records)

    assert ids_to_add == {"feat_001"}
    assert ids_to_modify == {"feat_002"}
    assert ids_to_delete == {"feat_003", "feat_006"}


def test_classify_changes_empty():
    """classify_changes handles empty input."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        a, m, d = classify_changes([])
    assert a == set()
    assert m == set()
    assert d == set()


def test_classify_changes_emits_deprecation_warning():
    """classify_changes emits a deprecation warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        classify_changes([])
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "query_changelog_ids" in str(w[0].message)


def test_bbox_filtering_logic():
    """Demonstrate bbox intersection logic used in DuckDB queries."""
    # Simulate what the DuckDB WHERE clause does
    query_bbox = BBox(-97.8, 30.2, -97.6, 30.4)

    # Record inside bbox
    inside = BBox(-97.75, 30.25, -97.74, 30.26)
    assert (
        inside.xmin <= query_bbox.xmax
        and inside.xmax >= query_bbox.xmin
        and inside.ymin <= query_bbox.ymax
        and inside.ymax >= query_bbox.ymin
    )

    # Record outside bbox
    outside = BBox(-98.0, 31.0, -97.9, 31.1)
    assert not (
        outside.xmin <= query_bbox.xmax
        and outside.xmax >= query_bbox.xmin
        and outside.ymin <= query_bbox.ymax
        and outside.ymax >= query_bbox.ymin
    )


def test_change_record_defaults():
    """ChangeRecord initialises with empty successor_ids and None bbox."""
    record = ChangeRecord(id="x", change_type=ChangeType.added)
    assert record.successor_ids == []
    assert record.bbox is None
