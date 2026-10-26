"""Silver sessions = batch sessionization (source of truth for session KPIs & ML labels).

REES46 already provides `user_session`; we aggregate every session touched by events of
day `ds` (looking ±1 day for sessions crossing midnight) and MERGE (upsert) on user_session.
"""
from __future__ import annotations

import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import SILVER_EVENTS, SILVER_SESSIONS
from ecom_common.transforms import count_if
from pipelines.batch.utils import day_range, delta_exists, parse_ds

log = get_logger("silver_sessions")


def build_sessions(events):
    return (
        events.groupBy("user_session")
        .agg(
            F.first("user_id", ignorenulls=True).alias("user_id"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count(F.lit(1)).alias("n_events"),
            count_if("view", "n_views"),
            count_if("cart", "n_carts"),
            count_if("remove_from_cart", "n_removes"),
            count_if("purchase", "n_purchases"),
            F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
            # net cart value, same definition as the online state (session_state.py)
            F.sum(F.when(F.col("event_type") == "cart", F.col("price"))
                   .when(F.col("event_type") == "remove_from_cart", -F.col("price"))
                   .otherwise(0.0)).alias("cart_value"),
            F.countDistinct("product_id").alias("n_distinct_products"),
            F.countDistinct(F.when(F.col("category_l1") != "unknown", F.col("category_l1")))
             .alias("n_distinct_categories"),
            F.max_by("category_l1", "event_time").alias("last_category_l1"),
            F.mode("category_l1").alias("main_category_l1"),
            F.max("is_bot").alias("is_bot"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_time"))).alias("first_purchase_time"),
        )
        .withColumn("session_date", F.to_date("session_start"))
        .withColumn("duration_s", F.col("session_end").cast("double") - F.col("session_start").cast("double"))
        .withColumn("converted", F.col("n_purchases") > 0)
        .withColumn("cart_abandoned", (F.col("n_carts") > 0) & (F.col("n_purchases") == 0))
        .withColumn("_updated_at", F.current_timestamp())
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv)
    spark = get_spark(f"silver_sessions_{args.ds}")
    events = spark.read.format("delta").load(SILVER_EVENTS.path)
    touched = events.where(F.col("event_date") == args.ds).select("user_session").distinct()
    window_days = day_range(args.ds_date, before=1, after=1)
    scope = events.where(F.col("event_date").isin(window_days)).join(touched, "user_session", "left_semi")
    sessions = build_sessions(scope)

    if not delta_exists(spark, SILVER_SESSIONS.path):
        sessions.write.format("delta").partitionBy(*SILVER_SESSIONS.partition_by).save(SILVER_SESSIONS.path)
    else:
        (DeltaTable.forPath(spark, SILVER_SESSIONS.path).alias("t")
            .merge(sessions.alias("s"), "t.user_session = s.user_session")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    log.info("silver.sessions upserted for %s", args.ds)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
