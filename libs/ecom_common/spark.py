"""SparkSession factory: static config lives in spark-defaults.conf, env-specific config here."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from ecom_common.config import get_settings


def get_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    s = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(s.spark_master)
        # Delta (also in spark-defaults; repeated so local/pytest sessions work without the image)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", s.s3.endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", s.s3.region)
        .config("spark.hadoop.fs.s3a.access.key", s.s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s.s3.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s.s3.endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # When the driver runs inside Airflow, executors must be able to call back to it.
    driver_host = os.getenv("SPARK_DRIVER_HOST")
    if driver_host:
        builder = builder.config("spark.driver.host", driver_host).config("spark.driver.bindAddress", "0.0.0.0")
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
