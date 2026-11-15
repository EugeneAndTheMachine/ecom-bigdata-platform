"""Q4 — Bot detection: events per user per 10-second tumbling window, flag z-score outliers.

z = (count - mean) / std computed across all users active in the same window. A minimum
absolute count guards against tiny populations.

APPEND mode: a window is emitted once, when the watermark passes its end, with its FINAL
counts and ALL of its users in the same micro-batch. (UPDATE mode would score partial
counts against only the users that changed in that trigger, and re-alert the same user
on every trigger.) Cost: an alert lands ≈ watermark + 10 s after the burst.
This rule is the online stage; the batch layer can refine it (e.g. Isolation Forest) and
Silver uses the union of online alerts to exclude bots (see pipelines/batch/silver_events.py).
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from pipelines.streaming.common import clean_events
from pipelines.streaming.sinks import clickhouse, kafka_sink, redis_sink

NAME = "bot_detection"


def build(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", "10 seconds"), "user_id")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .select(F.col("window.start").alias("window_start"), "user_id", "event_count")
    )


def score(pdf: pd.DataFrame, z_threshold: float, min_events: int) -> pd.DataFrame:
    """Pure function (unit-tested): returns flagged rows with a zscore column."""
    if pdf.empty:
        return pdf.assign(zscore=pd.Series(dtype="float64"))
    stats = pdf.groupby("window_start")["event_count"].agg(["mean", "std"]).reset_index()
    out = pdf.merge(stats, on="window_start", how="left")
    out["std"] = out["std"].fillna(0.0).replace(0.0, 1.0)
    out["zscore"] = (out["event_count"] - out["mean"]) / out["std"]
    flagged = out[(out["zscore"] > z_threshold) & (out["event_count"] >= min_events)]
    return flagged[["window_start", "user_id", "event_count", "zscore"]]


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        flagged = score(batch_df.toPandas(), s.streaming.bot_zscore_threshold, s.streaming.bot_min_events_10s)
        if flagged.empty:
            return
        flagged = flagged.assign(detected_at=pd.Timestamp.now(tz="UTC").tz_localize(None))
        clickhouse.insert_df(s, "rt_bot_alerts", flagged, version_col=None)
        redis_sink.flag_bots(redis_sink.get_client(s), flagged["user_id"].tolist())
        alerts = spark.createDataFrame(flagged.astype({"event_count": "int64"}))
        kafka_sink.publish_json(alerts, s, s.kafka.topic_alert_bot, key_col="user_id")

    return (
        build(clean_events(spark, s)).writeStream.outputMode("append")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )
