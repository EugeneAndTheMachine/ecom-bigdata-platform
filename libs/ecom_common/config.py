"""Typed, environment-driven configuration (12-factor). One source of truth for every job."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    schema_registry_url: str = field(default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"))
    topic_clickstream: str = field(default_factory=lambda: _env("TOPIC_CLICKSTREAM", "ecom.clickstream.v1"))
    topic_dlq: str = field(default_factory=lambda: _env("TOPIC_CLICKSTREAM_DLQ", "ecom.clickstream.dlq"))
    topic_sessions: str = field(default_factory=lambda: _env("TOPIC_SESSIONS", "agg.sessions"))
    topic_alert_cart: str = field(default_factory=lambda: _env("TOPIC_ALERT_CART", "alerts.cart_abandon"))
    topic_alert_bot: str = field(default_factory=lambda: _env("TOPIC_ALERT_BOT", "alerts.bot"))
    topic_reco_impressions: str = field(default_factory=lambda: _env("TOPIC_RECO_IMPRESSIONS", "reco.impressions"))


@dataclass(frozen=True)
class S3Settings:
    endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://minio:9000"))
    region: str = field(default_factory=lambda: _env("S3_REGION", "us-east-1"))
    access_key: str = field(default_factory=lambda: _env("MINIO_ROOT_USER", "minioadmin"))
    secret_key: str = field(default_factory=lambda: _env("MINIO_ROOT_PASSWORD", "minioadmin123"))
    lakehouse_bucket: str = field(default_factory=lambda: _env("LAKEHOUSE_BUCKET", "lakehouse"))
    checkpoint_bucket: str = field(default_factory=lambda: _env("CHECKPOINT_BUCKET", "checkpoints"))

    @property
    def lakehouse_root(self) -> str:
        # LAKEHOUSE_ROOT override = local integration tests (file:///tmp/lakehouse)
        return os.getenv("LAKEHOUSE_ROOT") or f"s3a://{self.lakehouse_bucket}"

    @property
    def checkpoint_root(self) -> str:
        return os.getenv("CHECKPOINT_ROOT") or f"s3a://{self.checkpoint_bucket}"


@dataclass(frozen=True)
class ServingSettings:
    redis_host: str = field(default_factory=lambda: _env("REDIS_HOST", "redis"))
    redis_port: int = field(default_factory=lambda: int(_env("REDIS_PORT_INTERNAL", "6379")))
    redis_password: str = field(default_factory=lambda: _env("REDIS_PASSWORD", ""))
    clickhouse_host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "clickhouse"))
    clickhouse_port: int = field(default_factory=lambda: int(_env("CLICKHOUSE_HTTP_PORT_INTERNAL", "8123")))
    clickhouse_user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = field(default_factory=lambda: _env("CLICKHOUSE_PASSWORD", ""))
    clickhouse_db: str = field(default_factory=lambda: _env("CLICKHOUSE_DB", "realtime"))
    trino_host: str = field(default_factory=lambda: _env("TRINO_HOST", "trino"))
    trino_port: int = field(default_factory=lambda: int(_env("TRINO_INTERNAL_PORT", "8080")))
    trino_catalog: str = field(default_factory=lambda: _env("TRINO_CATALOG", "delta"))


@dataclass(frozen=True)
class StreamingSettings:
    trigger_seconds: int = field(default_factory=lambda: int(_env("STREAMING_TRIGGER_SECONDS", "10")))
    watermark: str = field(default_factory=lambda: _env("STREAMING_WATERMARK", "2 minutes"))
    max_offsets_per_trigger: int = field(default_factory=lambda: int(_env("STREAMING_MAX_OFFSETS_PER_TRIGGER", "200000")))
    trending_top_k: int = field(default_factory=lambda: int(_env("TRENDING_TOP_K", "20")))
    bot_zscore_threshold: float = field(default_factory=lambda: float(_env("BOT_ZSCORE_THRESHOLD", "3.0")))
    bot_min_events_10s: int = field(default_factory=lambda: int(_env("BOT_MIN_EVENTS_10S", "20")))
    cart_abandon_timeout_minutes: int = field(default_factory=lambda: int(_env("CART_ABANDON_TIMEOUT_MINUTES", "30")))


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    s3: S3Settings = field(default_factory=S3Settings)
    serving: ServingSettings = field(default_factory=ServingSettings)
    streaming: StreamingSettings = field(default_factory=StreamingSettings)
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER_URL", "local[*]"))
    mlflow_tracking_uri: str = field(default_factory=lambda: _env("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    recs_top_n: int = field(default_factory=lambda: int(_env("RECS_TOP_N", "50")))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
