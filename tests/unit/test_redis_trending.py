"""trending:{cat} must never regress to an older window when a micro-batch only carries late events."""
from __future__ import annotations

import pytest

fakeredis = pytest.importorskip("fakeredis")

from pipelines.streaming.sinks.redis_sink import write_trending  # noqa: E402

WINDOW_S, SLIDE_S = 600, 60


def _event(cat: str, product_id: int, score: float, minute: int) -> list[tuple[str, int, int, float]]:
    """One event in minute `minute` belongs to the 10 sliding windows ending minute+1 .. minute+10."""
    return [(cat, 60 * (minute + k), product_id, score) for k in range(1, 11)]


def _write(r, rows):
    write_trending(r, rows, window_s=WINDOW_S, slide_s=SLIDE_S)


def test_newer_window_is_published():
    r = fakeredis.FakeRedis(decode_responses=True)
    _write(r, _event("electronics", 1, 1.0, minute=0))
    _write(r, _event("electronics", 2, 3.0, minute=5))
    assert r.zscore("trending:categories", "electronics") == 60 * 6
    assert dict(r.zrange("trending:electronics", 0, -1, withscores=True)) == {"1": 1.0, "2": 3.0}


def test_late_only_batch_does_not_replace_newer_window():
    r = fakeredis.FakeRedis(decode_responses=True)
    _write(r, _event("electronics", 2, 3.0, minute=20))
    _write(r, _event("electronics", 1, 50.0, minute=5))           # late event, older windows only
    assert r.zscore("trending:categories", "electronics") == 60 * 21
    assert dict(r.zrange("trending:electronics", 0, -1, withscores=True)) == {"2": 3.0}
    assert r.zscore(f"trending:electronics:w:{60 * 6}", "1") == 50.0   # history window still updated


def test_empty_batch_is_a_noop():
    r = fakeredis.FakeRedis(decode_responses=True)
    _write(r, [])
    assert r.keys("*") == []
