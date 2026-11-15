"""Q2 — Trending products: 10-minute sliding window (slide 1 minute), weighted score
view=1, cart=3, purchase=5 -> Redis trending:{category} (top-K reads) + ClickHouse history.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from ecom_common.transforms import count_if, event_weight
from pipelines.streaming.common import clean_events, window_cols
from pipelines.streaming.sinks import clickhouse, redis_sink

NAME = "trending_10m"
COLUMNS = ["window_start", "window_end", "category_l1", "product_id", "views", "carts", "purchases", "score"]
WINDOW_S = 600
SLIDE_S = 60


def build(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", f"{WINDOW_S} seconds", f"{SLIDE_S} seconds"),
                       "category_l1", "product_id")
        .agg(count_if("view", "views"), count_if("cart", "carts"), count_if("purchase", "purchases"),
             F.sum(event_weight()).alias("score"))
        .select(*window_cols(), "category_l1", "product_id", "views", "carts", "purchases", "score")
    )


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        pdf = batch_df.select(*COLUMNS).toPandas()
        if pdf.empty:
            return
        clickhouse.insert_df(s, "rt_trending_10m", pdf)
        # Redis gets only the top-K per (category, window) to keep ZSETs small.
        top = (pdf.sort_values("score", ascending=False)
                  .groupby(["category_l1", "window_end"], sort=False)
                  .head(s.streaming.trending_top_k * 5))
        rows = ((r.category_l1, int(r.window_end.timestamp()), int(r.product_id), float(r.score))
                for r in top.itertuples(index=False))
        redis_sink.write_trending(redis_sink.get_client(s), rows, WINDOW_S, SLIDE_S)

    return (
        build(clean_events(spark, s)).writeStream.outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )
