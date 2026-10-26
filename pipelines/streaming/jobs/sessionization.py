"""Sessionization — session window (gap 30 min). APPEND mode: a session is emitted once,
when the watermark passes its end (≈ 30 min + watermark after the last event).
-> ClickHouse rt_sessions + Kafka agg.sessions (landed to Bronze by Kafka Connect).

Live (in-progress) session features are produced by `session_state.py` instead, because
session windows do not support UPDATE mode.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from ecom_common.transforms import count_if
from pipelines.streaming.common import clean_events
from pipelines.streaming.sinks import clickhouse, kafka_sink

NAME = "sessions_closed"
GAP = "30 minutes"


def build(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.session_window("event_time", GAP).alias("sw"), "user_session", "user_id")
        .agg(
            F.count(F.lit(1)).alias("n_events"),
            count_if("view", "n_views"),
            count_if("cart", "n_carts"),
            count_if("purchase", "n_purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
        )
        .withColumn("duration_s", F.col("session_end").cast("double") - F.col("session_start").cast("double"))
        .drop("sw")
    )


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    cols = ["user_session", "user_id", "session_start", "session_end", "duration_s",
            "n_events", "n_views", "n_carts", "n_purchases", "revenue"]

    def sink(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.select(*cols).persist()
        clickhouse.insert_df(s, "rt_sessions", batch_df.toPandas())
        kafka_sink.publish_json(batch_df, s, s.kafka.topic_sessions, key_col="user_session")
        batch_df.unpersist()

    return (
        build(clean_events(spark, s)).writeStream.outputMode("append")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )
