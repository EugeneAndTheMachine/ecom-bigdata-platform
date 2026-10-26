"""Q3 — Live session features + cart abandonment with arbitrary stateful processing
(applyInPandasWithState, event-time timeout).

Per user_session we keep counters in the state store (RocksDB). On every micro-batch:
  * kind='update'  -> Redis session:{id}   (online features for the Recommend API)
On event-time timeout (no event for CART_ABANDON_TIMEOUT_MINUTES after watermark):
  * if cart_items > 0 and no purchase -> kind='abandon' -> Kafka alerts.cart_abandon + ClickHouse
"""
from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from pipelines.streaming.common import clean_events
from pipelines.streaming.sinks import clickhouse, kafka_sink, redis_sink

NAME = "session_state"
MAX_RECENT_CATEGORIES = 5

STATE_SCHEMA = StructType([
    StructField("user_id", LongType()),
    StructField("n_views", LongType()),
    StructField("n_carts", LongType()),
    StructField("n_removes", LongType()),
    StructField("n_purchases", LongType()),
    StructField("cart_value", DoubleType()),
    StructField("recent_categories", StringType()),   # pipe-separated, newest first
    StructField("first_ts_ms", LongType()),
    StructField("last_ts_ms", LongType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("kind", StringType()),
    StructField("user_session", StringType()),
    StructField("user_id", LongType()),
    StructField("n_views", LongType()),
    StructField("n_carts", LongType()),
    StructField("n_purchases", LongType()),
    StructField("cart_items", LongType()),
    StructField("cart_value", DoubleType()),
    StructField("last_category", StringType()),
    StructField("recent_categories", StringType()),
    StructField("first_event_time", TimestampType()),
    StructField("last_event_time", TimestampType()),
    StructField("duration_s", DoubleType()),
])


def _row(kind: str, session: str, st: tuple) -> dict:
    user_id, nv, nc, nr, np_, value, recent, first_ms, last_ms = st
    recent_list = recent.split("|") if recent else []
    return {
        "kind": kind, "user_session": session, "user_id": user_id,
        "n_views": nv, "n_carts": nc, "n_purchases": np_,
        "cart_items": max(nc - nr, 0), "cart_value": max(value, 0.0),
        "last_category": recent_list[0] if recent_list else None,
        "recent_categories": recent,
        "first_event_time": pd.Timestamp(first_ms, unit="ms"),
        "last_event_time": pd.Timestamp(last_ms, unit="ms"),
        "duration_s": (last_ms - first_ms) / 1000.0,
    }


def make_state_fn(timeout_ms: int):
    def update_session(key: tuple, pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
        (session,) = key
        if state.hasTimedOut:
            st = state.get
            state.remove()
            row = _row("abandon", session, st)
            if row["cart_items"] > 0 and row["n_purchases"] == 0:
                yield pd.DataFrame([row])
            return

        if state.exists:
            user_id, nv, nc, nr, np_, value, recent, first_ms, last_ms = state.get
        else:
            user_id, nv, nc, nr, np_, value, recent, first_ms, last_ms = (None, 0, 0, 0, 0, 0.0, "", None, None)
        cats = recent.split("|") if recent else []

        for pdf in pdfs:
            for ev in pdf.sort_values("event_time").itertuples(index=False):
                ts_ms = int(pd.Timestamp(ev.event_time).value // 1_000_000)
                user_id = int(ev.user_id)
                first_ms = ts_ms if first_ms is None else min(first_ms, ts_ms)
                last_ms = ts_ms if last_ms is None else max(last_ms, ts_ms)
                if ev.event_type == "view":
                    nv += 1
                elif ev.event_type == "cart":
                    nc += 1
                    value += float(ev.price)
                elif ev.event_type == "remove_from_cart":
                    nr += 1
                    value -= float(ev.price)
                elif ev.event_type == "purchase":
                    np_ += 1
                cat = ev.category_l1
                if cat and cat != "unknown":
                    cats = [cat] + [c for c in cats if c != cat]
        st = (user_id, nv, nc, nr, np_, value, "|".join(cats[:MAX_RECENT_CATEGORIES]), first_ms, last_ms)
        state.update(st)
        # timeout must be > current watermark, otherwise Spark raises
        state.setTimeoutTimestamp(max(last_ms + timeout_ms, state.getCurrentWatermarkMs() + 1))
        yield pd.DataFrame([_row("update", session, st)])

    return update_session


def build(events: DataFrame, timeout_minutes: int) -> DataFrame:
    return (
        events.select("user_session", "user_id", "event_type", "price", "category_l1", "event_time")
        .groupBy("user_session")
        .applyInPandasWithState(
            make_state_fn(timeout_minutes * 60_000),
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="update",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    redis_conf = (s.serving.redis_host, s.serving.redis_port, s.serving.redis_password)

    def write_partition(rows) -> None:  # runs on executors
        r = redis_sink.client_from(*redis_conf)
        redis_sink.write_sessions(r, (row.asDict() for row in rows))

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        updates = batch_df.where("kind = 'update'").drop("kind")
        updates.foreachPartition(write_partition)

        abandons = (batch_df.where("kind = 'abandon'")
                    .select("user_session", "user_id", F.col("cart_items").cast("int").alias("cart_items"),
                            "cart_value", "last_category", "last_event_time",
                            F.current_timestamp().alias("alerted_at")))
        if not abandons.isEmpty():
            kafka_sink.publish_json(abandons, s, s.kafka.topic_alert_cart, key_col="user_id")
            pdf = abandons.drop("last_category").toPandas()
            clickhouse.insert_df(s, "rt_cart_abandon", pdf, version_col=None)
        batch_df.unpersist()

    return (
        build(clean_events(spark, s), s.streaming.cart_abandon_timeout_minutes)
        .writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )

