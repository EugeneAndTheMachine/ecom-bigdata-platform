"""Bronze -> Silver events (+ silver.bot_users, silver.dq_results, quarantine).

Steps for logical date `ds` (= bronze ingest_date):
 1. read bronze.clickstream where ingest_date = ds
 2. DQ gate: row checks -> quarantine; abort if invalid ratio > MAX_INVALID_RATIO
 3. conform: types, brand normalisation, category levels, event_date, arrived_late flag
 4. bots: online alerts (bronze.alerts_bot) ∪ offline rule (events/minute) -> silver.bot_users
 5. dedup on the natural key and MERGE (insert-only) into silver.events
    -> idempotent; late events arriving days later are merged into their own event_date.
"""
from __future__ import annotations

import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import (
    BRONZE_ALERTS_BOT,
    BRONZE_CLICKSTREAM,
    SILVER_BOT_USERS,
    SILVER_DQ_RESULTS,
    SILVER_EVENTS,
    quarantine_path,
)
from ecom_common.transforms import category_levels, natural_key, normalize_brand
from pipelines.batch.utils import day_range, delta_exists, overwrite_partition, parse_ds
from pipelines.quality.checks import CLICKSTREAM_CHECKS, run_checks

log = get_logger("silver_events")
MAX_INVALID_RATIO = 0.05
OFFLINE_BOT_EVENTS_PER_MINUTE = 60   # a human rarely produces > 1 event / second for a full minute

SILVER_COLUMNS = [
    "event_id", "event_time", "event_date", "event_hour", "original_event_time", "event_type",
    "product_id", "category_id", "category_code", "category_l1", "category_l2", "category_l3",
    "brand", "price", "user_id", "user_session", "is_bot", "arrived_late", "chaos_tag",
    "_source", "_ingest_ts",
]


def conform(df: DataFrame, watermark_seconds: int) -> DataFrame:
    return (
        df.withColumn("brand", normalize_brand())
        .withColumn("category_code", F.lower(F.trim("category_code")))
        .select("*", *category_levels())
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        # TRUE if the event reached Kafka later than the stream watermark tolerated
        .withColumn("arrived_late", F.coalesce(
            F.col("_kafka_ts").cast("long") - F.col("event_time").cast("long") > F.lit(watermark_seconds),
            F.lit(False)))
    )


def detect_offline_bots(df: DataFrame) -> DataFrame:
    per_minute = (df.groupBy("user_id", F.date_trunc("minute", "event_time").alias("minute"))
                    .count().where(F.col("count") > OFFLINE_BOT_EVENTS_PER_MINUTE))
    return per_minute.select("user_id", F.to_date("minute").alias("event_date")).distinct() \
                     .withColumn("source", F.lit("offline_rule"))


def online_bots(spark: SparkSession, ds) -> DataFrame | None:
    if not delta_exists(spark, BRONZE_ALERTS_BOT.path):
        return None
    days = day_range(ds, before=1, after=1)
    return (spark.read.format("delta").load(BRONZE_ALERTS_BOT.path)
            .where(F.col("ingest_date").isin(days))
            .select("user_id", F.to_date(F.to_timestamp("window_start")).alias("event_date"))
            .distinct().withColumn("source", F.lit("stream_alert")))


def merge_events(spark: SparkSession, batch: DataFrame) -> None:
    if not delta_exists(spark, SILVER_EVENTS.path):
        batch.write.format("delta").partitionBy(*SILVER_EVENTS.partition_by).save(SILVER_EVENTS.path)
        return
    dates = [r.event_date.isoformat() for r in batch.select("event_date").distinct().collect()]
    date_list = ",".join(f"'{d}'" for d in dates) or "'1970-01-01'"
    cond = " AND ".join([f"t.event_date IN ({date_list})", "t.event_date = s.event_date"]
                        + [f"t.{k} = s.{k}" for k in natural_key()])
    (DeltaTable.forPath(spark, SILVER_EVENTS.path).alias("t")
        .merge(batch.alias("s"), cond)
        .whenMatchedUpdate(set={"is_bot": "t.is_bot OR s.is_bot"})   # bot flag can only be raised
        .whenNotMatchedInsertAll()
        .execute())


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv)
    s = get_settings()
    spark = get_spark(f"silver_events_{args.ds}")
    wm_seconds = int(s.streaming.watermark.split()[0]) * (60 if "minute" in s.streaming.watermark else 1)

    bronze = (spark.read.format("delta").load(BRONZE_CLICKSTREAM.path)
              .where(F.col("ingest_date") == args.ds))

    # 2) DQ gate ------------------------------------------------------------------------
    dq = run_checks(bronze, CLICKSTREAM_CHECKS)
    log.info("DQ %s: total=%d invalid=%d %s", args.ds, dq.total, dq.n_invalid, dq.summary)
    if dq.total == 0:
        log.warning("no bronze rows for %s — nothing to do", args.ds)
        return 0
    overwrite_partition(dq.summary_df(spark, "bronze.clickstream", args.ds_date),
                        SILVER_DQ_RESULTS, f"run_date = '{args.ds}'")
    if dq.n_invalid:
        dq.invalid.withColumn("quarantine_date", F.lit(args.ds).cast("date")) \
          .write.format("delta").mode("append").save(quarantine_path(SILVER_EVENTS))
    dq.assert_gate(MAX_INVALID_RATIO)

    # 3) conform -------------------------------------------------------------------------
    events = conform(dq.valid, wm_seconds)

    # 4) bots ---------------------------------------------------------------------------
    bots = detect_offline_bots(events)
    online = online_bots(spark, args.ds_date)
    if online is not None:
        bots = bots.unionByName(online)
    bots = bots.groupBy("user_id", "event_date").agg(F.collect_set("source").alias("sources")).cache()
    if bots.take(1):
        overwrite_partition(bots, SILVER_BOT_USERS,
                            "event_date IN (" + ",".join(f"'{d}'" for d in day_range(args.ds_date, 1, 1)) + ")")
    events = (events.join(bots.select("user_id", "event_date", F.lit(True).alias("is_bot")),
                          ["user_id", "event_date"], "left")
                    .withColumn("is_bot", F.coalesce("is_bot", F.lit(False))))

    # 5) dedup + merge ------------------------------------------------------------------
    w = Window.partitionBy(*natural_key()).orderBy(F.col("_ingest_ts"), F.col("event_id"))
    deduped = (events.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
                     .select(*SILVER_COLUMNS))
    merge_events(spark, deduped)
    log.info("silver.events merged for ingest_date=%s", args.ds)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
