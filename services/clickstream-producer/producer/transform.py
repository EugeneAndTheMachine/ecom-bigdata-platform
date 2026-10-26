"""Replay clock + record mapping.

shift mode   : event_time = wall_start + (original - first_original) / speed
               -> the stream looks "live" (dashboards on now(), watermarks behave like prod)
original mode: event_time = original timestamp (useful for deterministic tests / backfills)
In both modes the producer is *paced* by the same schedule (see rate_limiter.py).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

VALID_EVENT_TYPES = frozenset({"view", "cart", "remove_from_cart", "purchase"})


class InvalidEvent(ValueError):
    def __init__(self, reason: str, row: dict[str, Any]):
        super().__init__(reason)
        self.reason = reason
        self.row = row


@dataclass
class ReplayClock:
    speed: float
    time_mode: str = "shift"
    wall_start: datetime = field(default_factory=lambda: datetime.now(UTC))
    first_original: datetime | None = None

    def scheduled_wall_time(self, original: datetime) -> datetime:
        """When (wall clock) this event should be emitted."""
        if self.first_original is None:
            self.first_original = original
        if self.speed <= 0:
            return datetime.now(UTC)
        elapsed = (original - self.first_original).total_seconds() / self.speed
        return self.wall_start + timedelta(seconds=elapsed)

    def event_time(self, original: datetime) -> datetime:
        if self.time_mode == "original":
            return original
        return self.scheduled_wall_time(original)


def to_record(row: dict[str, Any], clock: ReplayClock) -> dict[str, Any]:
    """Map a REES46 CSV row to the Avro record. Raises InvalidEvent for DLQ routing."""
    if row.get("event_type") not in VALID_EVENT_TYPES:
        raise InvalidEvent("invalid_event_type", row)
    if row.get("price") is None or row["price"] < 0:
        raise InvalidEvent("invalid_price", row)
    if not row.get("user_session"):
        raise InvalidEvent("missing_user_session", row)
    if row.get("user_id") is None or row.get("product_id") is None:
        raise InvalidEvent("missing_ids", row)

    original: datetime = row["event_time"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": clock.event_time(original),
        "original_event_time": original,
        "event_type": row["event_type"],
        "product_id": int(row["product_id"]),
        "category_id": int(row["category_id"]) if row.get("category_id") is not None else 0,
        "category_code": row.get("category_code"),
        "brand": row.get("brand"),
        "price": float(row["price"]),
        "user_id": int(row["user_id"]),
        "user_session": row["user_session"],
        "chaos_tag": None,
    }
