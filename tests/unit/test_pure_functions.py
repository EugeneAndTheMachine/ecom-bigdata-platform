"""Unit tests for the pure (Spark-free) logic inside jobs.
Modules import pyspark at top level, so these run where pyspark is installed (CI / Spark image):
    pip install -r requirements-dev.txt && make test
"""
from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("pyspark")


def test_bot_score_flags_only_outliers_above_min_events():
    from pipelines.streaming.jobs.bot_detection import score

    w = pd.Timestamp("2026-10-26 10:00")
    normal = [{"window_start": w, "user_id": i, "event_count": 5 + i % 3} for i in range(50)]
    bot = [{"window_start": w, "user_id": 999, "event_count": 400}]
    out = score(pd.DataFrame(normal + bot), z_threshold=4.0, min_events=30)
    assert out["user_id"].tolist() == [999]
    assert out["zscore"].iloc[0] > 4


def test_bot_score_empty_frame():
    from pipelines.streaming.jobs.bot_detection import score

    out = score(pd.DataFrame(columns=["window_start", "user_id", "event_count"]), 4.0, 30)
    assert out.empty and "zscore" in out.columns


def test_label_segments_orders_by_value():
    from pipelines.ml.segmentation import label_segments

    # (recency, frequency, monetary) standardized
    centers = [[2.0, -1.0, -1.0], [-1.0, 2.0, 2.0], [0.0, 0.0, 0.0]]
    labels = label_segments(centers, 3)
    assert labels[1] == "champions" and labels[0] != "champions"
    assert len(set(labels.values())) == 3


def test_reconciliation_compare_marks_warn():
    from pipelines.batch.reconciliation import METRICS, compare

    base = {m: 100 for m in METRICS}
    batch = pd.DataFrame([{"hour": 10, **base, "late_events": 3}])
    stream = pd.DataFrame([{"hour": 10, **{m: 90 for m in METRICS}, "enriched_purchases": 100}])
    out = compare(batch, stream, "2026-10-25", warn_pct=0.02)
    assert set(out["status"]) <= {"ok", "warn"}
    assert (out.loc[out["metric"] == METRICS[0], "status"] == "warn").all()


class FakeState:
    """Minimal GroupState stand-in for applyInPandasWithState unit tests."""

    def __init__(self, value=None, timed_out=False, watermark_ms=0):
        self._value, self.hasTimedOut, self._wm, self.timeout = value, timed_out, watermark_ms, None

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        return self._value

    def update(self, v):
        self._value = v

    def remove(self):
        self._value = None

    def getCurrentWatermarkMs(self):
        return self._wm

    def setTimeoutTimestamp(self, ts):
        self.timeout = ts


def test_session_state_update_then_abandon():
    from pipelines.streaming.jobs.session_state import make_state_fn

    fn = make_state_fn(timeout_ms=30 * 60_000)
    events = pd.DataFrame([
        {"event_time": pd.Timestamp("2026-10-26 10:00:00"), "user_id": 1, "event_type": "view",
         "price": 10.0, "category_l1": "electronics"},
        {"event_time": pd.Timestamp("2026-10-26 10:01:00"), "user_id": 1, "event_type": "cart",
         "price": 10.0, "category_l1": "electronics"},
    ])
    state = FakeState()
    upd = pd.concat(list(fn(("s1",), iter([events]), state)))
    assert upd.iloc[0]["kind"] == "update" and upd.iloc[0]["cart_items"] == 1
    assert state.timeout is not None

    state.hasTimedOut = True
    out = list(fn(("s1",), iter([]), state))
    assert out and out[0].iloc[0]["kind"] == "abandon" and not state.exists
