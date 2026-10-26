"""Q1 — Real-time funnel: 1-minute tumbling window x category_l1 -> ClickHouse rt_funnel_1m.

Output mode UPDATE: every trigger re-emits the full counts of windows touched in that
micro-batch; ReplacingMergeTree keeps the latest version per (window_start, category_l1).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from ecom_common.transforms import funnel_aggs
from pipelines.streaming.common import clean_events, window_cols
from pipelines.streaming.sinks import clickhouse

NAME = "funnel_1m"
COLUMNS = ["window_start", "window_end", "category_l1", "views", "carts", "removes", "purchases", "revenue"]


def build(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", "1 minute"), "category_l1")
        .agg(*funnel_aggs())
        .select(*window_cols(), "category_l1", "views", "carts", "removes", "purchases", "revenue")
    )


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        clickhouse.insert_df(s, "rt_funnel_1m", batch_df.select(*COLUMNS).toPandas())

    return (
        build(clean_events(spark, s)).writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )
