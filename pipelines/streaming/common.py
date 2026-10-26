"""Shared ingest stage of the hot path:  Kafka -> Avro decode -> validate -> watermark -> dedup.

Each streaming query calls `clean_events()` and therefore owns its Kafka source and its
checkpoint (independent restart / replay). The parse logic is identical for all of them.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from ecom_common.avro import decode_confluent_avro
from ecom_common.config import Settings
from ecom_common.tables import checkpoint_path
from ecom_common.transforms import EVENT_TYPES, category_levels, normalize_brand


def read_kafka(spark: SparkSession, s: Settings, topic: str | None = None) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka.bootstrap_servers)
        .option("subscribe", topic or s.kafka.topic_clickstream)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", s.streaming.max_offsets_per_trigger)
        .load()
    )


def decode(raw: DataFrame) -> DataFrame:
    """Adds `e` (decoded struct or NULL) and `is_valid` without dropping anything."""
    decoded = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        "value", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
        decode_confluent_avro("value", "clickstream_event").alias("e"),
    )
    valid = (
        F.col("e").isNotNull()
        & F.col("e.event_type").isin(*EVENT_TYPES)
        & (F.col("e.price") >= 0)
        & F.col("e.user_session").isNotNull()
    )
    return decoded.withColumn("is_valid", valid)


def clean_events(spark: SparkSession, s: Settings) -> DataFrame:
    """Valid, typed, watermarked, deduplicated events (the common input of every job)."""
    events = (
        decode(read_kafka(spark, s))
        .where("is_valid")
        .select("e.*", "kafka_ts")
        .withColumn("brand", normalize_brand())
        .select("*", *category_levels())
        .withWatermark("event_time", s.streaming.watermark)
    )
    # Spark >= 3.5: state for event_id is evicted once the watermark passes it.
    return events.dropDuplicatesWithinWatermark(["event_id"])


def start_dlq_query(spark: SparkSession, s: Settings) -> StreamingQuery:
    """Bytes that cannot be decoded/validated by Spark -> DLQ topic (JSON, base64 payload)."""
    bad = (
        decode(read_kafka(spark, s))
        .where(~F.col("is_valid"))
        .select(
            F.col("kafka_key").alias("key"),
            F.to_json(F.struct(
                F.lit("spark_decode_or_validation").alias("reason"),
                F.col("partition"), F.col("offset"), F.col("kafka_ts"),
                F.base64("value").alias("payload_b64"),
            )).alias("value"),
        )
    )
    return (
        bad.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka.bootstrap_servers)
        .option("topic", s.kafka.topic_dlq)
        .option("checkpointLocation", checkpoint_path("dlq"))
        .queryName("dlq")
        .trigger(processingTime=f"{s.streaming.trigger_seconds} seconds")
        .start()
    )


def window_cols(window_col: str = "window") -> list:
    return [F.col(f"{window_col}.start").alias("window_start"), F.col(f"{window_col}.end").alias("window_end")]
