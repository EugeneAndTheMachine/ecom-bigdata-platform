"""Kafka publishing: Avro (Schema Registry) for the main topic, JSON for the DLQ."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from producer import metrics


def _json_default(o: Any) -> Any:
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


class ClickstreamPublisher:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str, topic: str,
                 dlq_topic: str, schema_path: str):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,      # no duplicates on retries
            "acks": "all",
            "linger.ms": 20,
            "batch.size": 256 * 1024,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
            "client.id": "rees46-replay",
        })
        registry = SchemaRegistryClient({"url": schema_registry_url})
        self._serializer = AvroSerializer(
            registry,
            Path(schema_path).read_text(),
            conf={"auto.register.schemas": True},
        )
        self._ctx = SerializationContext(topic, MessageField.VALUE)

    # ---------------------------------------------------------------------------------
    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            metrics.DELIVERY_ERRORS.inc()

    def send(self, record: dict[str, Any]) -> None:
        try:
            value = self._serializer(record, self._ctx)
        except (SerializationError, ValueError, TypeError) as exc:
            self.send_dlq(record, f"avro_validation:{type(exc).__name__}")
            return
        self._produce(self.topic, record["user_session"], value)
        metrics.EVENTS_SENT.labels(record["event_type"]).inc()

    def send_dlq(self, row: dict[str, Any], reason: str) -> None:
        payload = {"reason": reason, "payload": row}
        self._produce(self.dlq_topic, str(row.get("user_session") or ""),
                      json.dumps(payload, default=_json_default).encode())
        metrics.EVENTS_DLQ.labels(reason.split(":")[0]).inc()

    def _produce(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(topic, key=key.encode(), value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.5)  # local queue full -> back-pressure
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
