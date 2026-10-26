"""Publish a (batch) DataFrame to a Kafka topic as JSON — used inside foreachBatch."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom_common.config import Settings


def publish_json(df: DataFrame, s: Settings, topic: str, key_col: str) -> None:
    if df.isEmpty():
        return
    (
        df.select(
            F.col(key_col).cast("string").alias("key"),
            F.to_json(F.struct(*df.columns)).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka.bootstrap_servers)
        .option("topic", topic)
        .save()
    )
