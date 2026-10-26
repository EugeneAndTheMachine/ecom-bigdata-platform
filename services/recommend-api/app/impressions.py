"""Fire-and-forget impression logging to Kafka (closes the feedback loop: reco.impressions
-> Kafka Connect -> landing -> Bronze -> CTR / offline evaluation)."""
from __future__ import annotations

import json
import logging
import time

log = logging.getLogger("impressions")


class ImpressionLogger:
    def __init__(self, bootstrap: str, topic: str):
        self.topic = topic
        self.producer = None
        try:
            from confluent_kafka import Producer

            self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50,
                                      "acks": "1", "enable.idempotence": False})
        except Exception as exc:  # noqa: BLE001
            log.warning("impression logging disabled: %s", exc)

    def log(self, payload: dict) -> None:
        if self.producer is None:
            return
        payload = {**payload, "ts": time.time()}
        try:
            self.producer.produce(self.topic, key=str(payload.get("user_id")), value=json.dumps(payload))
            self.producer.poll(0)
        except BufferError:
            log.warning("impression buffer full, dropping")

    def close(self) -> None:
        if self.producer is not None:
            self.producer.flush(5)
