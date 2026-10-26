"""Landing (Kafka Connect files) -> Bronze Delta, one ingest_date partition per run.

    spark-submit pipelines/batch/bronze_load.py --ds 2026-10-26

* Bronze keeps the payload as received + lineage columns (_source, _kafka_*, _landing_file).
* Re-running the same ds replaces exactly that partition (replaceWhere) -> idempotent.
* Landing is partitioned by Kafka record time (= ingest time), so a late event lands in the
  partition of the day it ARRIVED — nothing is ever lost because a partition was closed.
"""
from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import BRONZE_ALERTS_BOT, BRONZE_ALERTS_CART, BRONZE_CLICKSTREAM, Table, landing_path
from pipelines.batch.utils import overwrite_partition, parse_ds

log = get_logger("bronze_load")

ALERT_BOT_SCHEMA = ("user_id long, window_start string, event_count long, zscore double, "
                    "detected_at string, _kafka_offset long, _kafka_partition int, _kafka_ts long")
ALERT_CART_SCHEMA = ("user_session string, user_id long, cart_items long, cart_value double, "
                     "last_category string, last_event_time string, alerted_at string, "
                     "_kafka_offset long, _kafka_partition int, _kafka_ts long")


def read_landing(spark: SparkSession, topic: str, ds: str, fmt: str, schema: str | None = None) -> DataFrame | None:
    base = landing_path(topic)
    reader = spark.read.option("basePath", base)
    if schema:
        reader = reader.schema(schema)
    try:
        df = reader.format(fmt).load(f"{base}/dt={ds}")
    except AnalysisException as exc:  # path does not exist -> nothing landed that day
        log.warning("no landing data for %s dt=%s (%s)", topic, ds, exc.desc[:120])
        return None
    return df.withColumn("_landing_file", F.input_file_name())


def with_lineage(df: DataFrame, ds: str, source: str) -> DataFrame:
    return (df.withColumn("_source", F.lit(source))
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("ingest_date", F.lit(ds).cast("date")))


def load(spark: SparkSession, topic: str, table: Table, ds: str, fmt: str, schema: str | None = None) -> int:
    df = read_landing(spark, topic, ds, fmt, schema)
    if df is None:
        return 0
    if "hr" in df.columns:
        df = df.drop("hr")
    if "dt" in df.columns:
        df = df.drop("dt")
    if "_kafka_ts" in df.columns and dict(df.dtypes)["_kafka_ts"] in ("bigint", "long"):
        df = df.withColumn("_kafka_ts", (F.col("_kafka_ts") / 1000).cast("timestamp"))
    out = with_lineage(df, ds, "kafka_connect")
    # scope by _source too: never touch rows written by bulk_backfill for the same date
    overwrite_partition(out, table, f"_source = 'kafka_connect' AND ingest_date = '{ds}'")
    n = out.count()
    log.info("bronze %s <- %s dt=%s : %d rows", table.fqn, topic, ds, n)
    return n


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv)
    s = get_settings()
    spark = get_spark(f"bronze_load_{args.ds}")
    load(spark, s.kafka.topic_clickstream, BRONZE_CLICKSTREAM, args.ds, "parquet")
    load(spark, s.kafka.topic_alert_bot, BRONZE_ALERTS_BOT, args.ds, "json", ALERT_BOT_SCHEMA)
    load(spark, s.kafka.topic_alert_cart, BRONZE_ALERTS_CART, args.ds, "json", ALERT_CART_SCHEMA)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
