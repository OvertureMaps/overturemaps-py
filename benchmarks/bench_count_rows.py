"""
Network benchmark for count_rows overhead.

Measures how long count_rows() takes for different bbox sizes,
isolated from the actual download time.

Requires network access (hits S3 directly).

Run:
    pytest benchmarks/bench_count_rows.py -v
"""

import pytest

from overturemaps.core import count_rows

BBOX_SMALL = (-71.068, 42.353, -71.058, 42.363)  # ~10 blocks
BBOX_BOSTON = (-71.191, 42.227, -70.985, 42.400)  # full city


@pytest.mark.network
def test_count_rows_small(benchmark):
    """count_rows overhead for a small bbox (~10 blocks)."""
    benchmark.pedantic(
        count_rows, args=("building", BBOX_SMALL), kwargs={"stac": True}, rounds=3, iterations=1
    )


@pytest.mark.network
def test_count_rows_city(benchmark):
    """count_rows overhead for a full city bbox."""
    benchmark.pedantic(
        count_rows, args=("building", BBOX_BOSTON), kwargs={"stac": True}, rounds=3, iterations=1
    )
