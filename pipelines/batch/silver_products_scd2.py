"""Product dimension as SCD Type 2 (price / brand / category history) — Delta MERGE.

REES46 prices change over time for the same product_id: a real SCD2 use case.
Daily snapshot = last observed attributes of each product on day `ds`; a new version is
opened when the attribute hash differs from the current version. Granularity is one day:
valid_from / valid_to are day boundaries, so an as-of join uses
`valid_from <= event_time AND (valid_to IS NULL OR event_time < valid_to)`.

Classic "staged updates" pattern (one atomic MERGE):
  * rows with merge_key = product_id  -> close the current version if changed
  * rows with merge_key = NULL        -> never match -> insert the new version
IMPORTANT: run days in order (Airflow: depends_on_past=True on this task).
"""
from __future__ import annotations

import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import SILVER_EVENTS, SILVER_PRODUCTS_SCD2
from pipelines.batch.utils import delta_exists, parse_ds

log = get_logger("silver_products_scd2")
ATTRS = ["category_id", "category_code", "category_l1", "category_l2", "category_l3", "brand", "price"]


def attr_hash() -> F.Column:
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(a).cast("string"), F.lit("∅")) for a in ATTRS]), 256)


def daily_snapshot(events: DataFrame) -> DataFrame:
    w = Window.partitionBy("product_id").orderBy(F.col("event_time").desc())
    return (events.where(~F.col("is_bot"))
            .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            # daily granularity: a version is valid from the start of the day it was observed
            .select("product_id", *ATTRS, F.date_trunc("day", "event_time").alias("observed_at"))
            .withColumn("attr_hash", attr_hash()))


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv)
    spark = get_spark(f"silver_products_scd2_{args.ds}")
    events = spark.read.format("delta").load(SILVER_EVENTS.path).where(F.col("event_date") == args.ds)
    updates = daily_snapshot(events)

    if not delta_exists(spark, SILVER_PRODUCTS_SCD2.path):
        (updates.withColumn("valid_from", F.col("observed_at"))
                .withColumn("valid_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .drop("observed_at")
                .write.format("delta").save(SILVER_PRODUCTS_SCD2.path))
        log.info("initial load of products_scd2 for %s", args.ds)
        return 0

    target = DeltaTable.forPath(spark, SILVER_PRODUCTS_SCD2.path)
    current = target.toDF().where("is_current").select("product_id", F.col("attr_hash").alias("cur_hash"))
    changed = (updates.join(current, "product_id")
               .where(F.col("attr_hash") != F.col("cur_hash")).drop("cur_hash"))

    staged = (changed.withColumn("merge_key", F.lit(None).cast("long"))
              .unionByName(updates.withColumn("merge_key", F.col("product_id"))))

    insert_values = {a: f"s.{a}" for a in ["product_id", *ATTRS, "attr_hash"]}
    insert_values.update({"valid_from": "s.observed_at", "valid_to": "NULL", "is_current": "true"})
    (target.alias("t")
        .merge(staged.alias("s"), "t.product_id = s.merge_key AND t.is_current")
        .whenMatchedUpdate(condition="t.attr_hash <> s.attr_hash",
                           set={"is_current": "false", "valid_to": "s.observed_at"})
        .whenNotMatchedInsert(values=insert_values)
        .execute())
    log.info("products_scd2 merged for %s (changed=%d)", args.ds, changed.count())
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
