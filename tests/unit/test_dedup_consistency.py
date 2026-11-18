"""Hot path and cold path must remove the SAME duplicates (otherwise reconciliation shows a
systematic gap that is not caused by late data). Runs a local Spark session (needs Java 17+)."""
from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ecom_common.transforms import event_id_col, natural_key  # noqa: E402
from producer.chaos import ChaosInjector  # noqa: E402
from producer.transform import ReplayClock, to_record  # noqa: E402

T0 = datetime(2019, 10, 1, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    # workers must run the same interpreter as the driver (avoids PYTHON_VERSION_MISMATCH)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("dedup-consistency")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def _row(session="s1", seconds=0, product=10, etype="view", price=1.0):
    return {"event_time": T0 + timedelta(seconds=seconds), "event_type": etype, "product_id": product,
            "category_id": 1, "category_code": None, "brand": None, "price": price,
            "user_id": 7, "user_session": session}


def _records():
    clock = ReplayClock(speed=10)
    rows = [
        _row(), _row(),                       # exact duplicate inside the source file
        _row(price=2.0),                      # same natural key, different price
        _row(seconds=1), _row(etype="cart"), _row(product=11), _row(session="s2"),
    ]
    out = [to_record(r, clock) for r in rows]
    chaos = ChaosInjector(late_ratio=0.0, dup_ratio=1.0, bot_burst_prob=0.0, late_delay_s=60)
    out += chaos.process(to_record(_row(seconds=5), clock), 0.0)   # replay duplicate (same event_id)
    return out


def _both_sides(spark, records):
    df = spark.createDataFrame([
        {k: r[k] for k in ("event_id", "event_time", "event_type", "product_id", "price", "user_session")}
        for r in records
    ])
    stream_side = df.dropDuplicates(["event_id"]).count()          # what the hot path keys on
    w = Window.partitionBy(*natural_key()).orderBy("event_id")    # what Silver keys on
    silver_side = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").count()
    return stream_side, silver_side


def test_stream_and_silver_dedup_keep_the_same_rows(spark):
    assert _both_sides(spark, _records()) == (6, 6)


def test_producer_restart_is_kept_by_both_layers(spark):
    rows = [_row(), _row(seconds=1)]
    clock1 = ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, tzinfo=UTC))
    clock2 = ReplayClock(speed=10, wall_start=datetime(2026, 1, 1, 1, tzinfo=UTC))   # restarted 1 h later
    run1 = [to_record(r, clock1) for r in rows]
    run2 = [to_record(r, clock2) for r in rows]
    assert _both_sides(spark, run1 + run2) == (4, 4)


def test_max_speed_replay_is_consistent(spark):
    clock = ReplayClock(speed=0)                                   # event_time = now() per record
    recs = [to_record(_row(), clock) for _ in range(3)]
    stream_side, silver_side = _both_sides(spark, recs)
    assert stream_side == silver_side


def test_producer_and_spark_compute_the_same_event_id(spark):
    recs = [to_record(_row(seconds=s, product=p), ReplayClock(speed=1))
            for s, p in [(0, 10), (59, 44600062), (3600, 1)]]
    recs.append(to_record(_row(seconds=1), ReplayClock(speed=3)))   # sub-second event_time
    df = spark.createDataFrame([
        {"user_session": r["user_session"], "event_time": r["event_time"],
         "product_id": r["product_id"], "event_type": r["event_type"], "expected": r["event_id"]}
        for r in recs
    ])
    mismatches = df.where(event_id_col("event_time") != F.col("expected")).count()
    assert mismatches == 0
