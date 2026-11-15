"""Sink logic that needs no Spark: Redis trending swap (fakeredis)."""
from __future__ import annotations

import pytest

fakeredis = pytest.importorskip("fakeredis")


def test_serving_window_is_the_full_last_ten_minutes():
    from pipelines.streaming.sinks.redis_sink import serving_window_end

    # newest event 12:00:30 -> windows end 12:01 .. 12:10; the full last 10 min ends at 12:01
    t_1201, t_1210 = 12 * 3600 + 60, 12 * 3600 + 600
    assert serving_window_end(t_1210, window_s=600, slide_s=60) == t_1201


def test_write_trending_publishes_full_window_not_newest():
    from pipelines.streaming.sinks.redis_sink import write_trending

    r = fakeredis.FakeRedis(decode_responses=True)
    ends = [60 * m for m in range(1, 11)]          # one event belongs to 10 sliding windows
    full = ends[0]
    rows = [("electronics", e, 1, 1.0) for e in ends]
    rows += [("electronics", full, 2, 7.0)]         # older activity only the full window holds
    write_trending(r, rows, window_s=600, slide_s=60)

    assert r.zrange("trending:electronics", 0, -1, withscores=True) == [("1", 1.0), ("2", 7.0)]
    assert r.zscore("trending:categories", "electronics") == full
