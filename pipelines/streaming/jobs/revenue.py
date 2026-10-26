"""Purchases enriched with the product dimension (stream–static join) -> ClickHouse rt_purchases.

Replaces the CDC `rt_orders` flow of the original design (scope = REES46 only): every
purchase event IS an order line. The dimension is re-read from Delta inside foreachBatch, so
SCD2 changes published by the nightly batch become visible without restarting the query.
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.utils import AnalysisException

from ecom_common.config import Settings
from ecom_common.tables import SILVER_PRODUCTS_SCD2, checkpoint_path
from pipelines.streaming.common import clean_events
from pipelines.streaming.sinks import clickhouse

NAME = "revenue_enriched"
COLUMNS = ["event_id", "event_time", "user_id", "user_session", "product_id", "category_l1",
           "category_code", "brand", "price", "list_price"]


def current_dim(spark: SparkSession) -> DataFrame | None:
    try:
        return (spark.read.format("delta").load(SILVER_PRODUCTS_SCD2.path)
                .where("is_current")
                .select("product_id", F.col("price").alias("list_price"),
                        F.col("brand").alias("dim_brand"), F.col("category_code").alias("dim_category_code")))
    except AnalysisException:
        return None  # first run: dimension not built yet


def enrich(batch: DataFrame, dim: DataFrame | None) -> DataFrame:
    if dim is None:
        return batch.withColumn("list_price", F.lit(None).cast("double"))
    joined = batch.join(F.broadcast(dim), "product_id", "left")
    return (joined
            .withColumn("brand", F.coalesce(F.col("brand"), F.col("dim_brand")))
            .withColumn("category_code", F.coalesce(F.col("category_code"), F.col("dim_category_code"))))


def start(spark: SparkSession, s: Settings) -> StreamingQuery:
    def sink(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        out = enrich(batch_df, current_dim(batch_df.sparkSession))
        pdf = out.select(*COLUMNS).fillna({"category_code": ""}).toPandas()
        pdf["ingested_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
        clickhouse.insert_df(s, "rt_purchases", pdf, version_col=None)

    purchases = clean_events(spark, s).where(F.col("event_type") == "purchase")
    return (
        purchases.writeStream.outputMode("append")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint_path(NAME))
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .queryName(NAME)
        .start()
    )
