"""Historical backfill: REES46 CSV(.gz) -> Bronze directly (bypasses Kafka).

Why: replaying 7 months through Kafka at 50x still takes days. ML needs the long history,
the hot path does not. Bulk rows keep their ORIGINAL timestamps (2019-10 .. 2020-04) and are
tagged _source='bulk_csv'; replayed rows (_source='kafka_connect') live on the "now" timeline.
Reconciliation only compares kafka_connect rows with the speed layer.

    spark-submit pipelines/batch/bulk_backfill.py --input /data/raw/2019-Oct.csv.gz
    spark-submit pipelines/batch/bulk_backfill.py --input 's3a://lakehouse/raw/rees46/*.csv.gz'
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import BRONZE_CLICKSTREAM

log = get_logger("bulk_backfill")

CSV_SCHEMA = ("event_time string, event_type string, product_id long, category_id long, "
              "category_code string, brand string, price double, user_id long, user_session string")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="file path / glob (local or s3a://)")
    args = p.parse_args(argv)

    spark = get_spark("bulk_backfill")
    raw = spark.read.option("header", True).schema(CSV_SCHEMA).csv(args.input)
    ts = F.to_timestamp(F.regexp_replace("event_time", " UTC$", ""), "yyyy-MM-dd HH:mm:ss")
    df = (
        raw.withColumn("event_time", ts)
        .withColumn("original_event_time", F.col("event_time"))
        # deterministic id => re-running the backfill never creates new identities
        .withColumn("event_id", F.sha2(F.concat_ws("|", "user_session", F.col("event_time").cast("string"),
                                                     "product_id", "event_type", "price"), 256))
        .withColumn("chaos_tag", F.lit(None).cast("string"))
        .withColumn("_kafka_partition", F.lit(None).cast("int"))
        .withColumn("_kafka_offset", F.lit(None).cast("long"))
        .withColumn("_kafka_ts", F.lit(None).cast("timestamp"))
        .withColumn("_landing_file", F.input_file_name())
        .withColumn("_source", F.lit("bulk_csv"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("event_time"))
    )
    bounds = df.agg(F.min("ingest_date").alias("lo"), F.max("ingest_date").alias("hi")).first()
    predicate = f"_source = 'bulk_csv' AND ingest_date BETWEEN '{bounds.lo}' AND '{bounds.hi}'"
    log.info("backfill %s -> %s (%s)", args.input, BRONZE_CLICKSTREAM.path, predicate)
    (df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
       .option("mergeSchema", "true").partitionBy("ingest_date").save(BRONZE_CLICKSTREAM.path))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
