from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ProducerConfig:
    input_path: str
    speed: float                 # 10 = 10x faster than real time; <= 0 = as fast as possible
    time_mode: str               # "shift" (event_time ~ now) | "original" (keep 2019/2020 timestamps)
    max_events: int              # 0 = unlimited
    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    dlq_topic: str
    schema_path: str
    metrics_port: int
    chaos_enabled: bool
    chaos_late_ratio: float
    chaos_dup_ratio: float
    chaos_bot_burst_prob: float
    chaos_late_delay_s: float
    seed: int

    @classmethod
    def from_env(cls) -> ProducerConfig:
        e = os.getenv
        return cls(
            input_path=e("REES46_INPUT", "/data/raw/2019-Oct.csv.gz"),
            speed=float(e("REPLAY_SPEED", "10")),
            time_mode=e("REPLAY_TIME_MODE", "shift"),
            max_events=int(e("REPLAY_MAX_EVENTS", "0")),
            bootstrap_servers=e("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            schema_registry_url=e("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=e("TOPIC_CLICKSTREAM", "ecom.clickstream.v1"),
            dlq_topic=e("TOPIC_CLICKSTREAM_DLQ", "ecom.clickstream.dlq"),
            schema_path=e("CLICKSTREAM_SCHEMA_PATH", "/opt/app/schemas/avro/clickstream_event.avsc"),
            metrics_port=int(e("METRICS_PORT", "8001")),
            chaos_enabled=_bool(e("CHAOS_ENABLED", "false")),
            chaos_late_ratio=float(e("CHAOS_LATE_RATIO", "0.02")),
            chaos_dup_ratio=float(e("CHAOS_DUP_RATIO", "0.01")),
            chaos_bot_burst_prob=float(e("CHAOS_BOT_BURST_PROB", "0.0005")),
            chaos_late_delay_s=float(e("CHAOS_LATE_DELAY_SECONDS", "240")),
            seed=int(e("CHAOS_SEED", "42")),
        )
