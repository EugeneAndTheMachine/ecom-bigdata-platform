"""Chaos injector — only enabled in test runs to PROVE that watermark, dedup and bot
filtering work. Every injected record carries `chaos_tag` = ground truth for evaluation.

  late : record is held back and emitted CHAOS_LATE_DELAY_SECONDS later with its original
         (old) event_time  -> arrives after the watermark, must be dropped by the hot path
         but recovered by the cold path (=> reconciliation gap).
  dup  : the same record (same event_id) is emitted twice -> dropDuplicatesWithinWatermark.
  bot  : burst of 50-200 'view' events by a synthetic user within ~10 s -> bot detection.
"""
from __future__ import annotations

import heapq
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

BOT_USER_ID_BASE = 2_000_000_000  # outside REES46 user_id range, still fits int32 for ALS


@dataclass(order=True)
class _Held:
    release_at: float
    seq: int
    record: dict[str, Any] = field(compare=False)


class ChaosInjector:
    def __init__(self, late_ratio: float, dup_ratio: float, bot_burst_prob: float,
                 late_delay_s: float, seed: int = 42, enabled: bool = True):
        self.enabled = enabled
        self.late_ratio = late_ratio
        self.dup_ratio = dup_ratio
        self.bot_burst_prob = bot_burst_prob
        self.late_delay_s = late_delay_s
        self._rng = random.Random(seed)
        self._held: list[_Held] = []
        self._seq = 0

    # -- per-record decisions -------------------------------------------------------
    def process(self, record: dict[str, Any], now_ts: float) -> list[dict[str, Any]]:
        """Return the records to emit NOW for this input record (0, 1 or 2+)."""
        if not self.enabled:
            return [record]
        out: list[dict[str, Any]] = []
        r = self._rng.random()
        if r < self.late_ratio:
            late = {**record, "chaos_tag": "late"}
            self._seq += 1
            heapq.heappush(self._held, _Held(now_ts + self.late_delay_s, self._seq, late))
        elif r < self.late_ratio + self.dup_ratio:
            out.extend([record, {**record, "chaos_tag": "dup"}])
        else:
            out.append(record)
        if self._rng.random() < self.bot_burst_prob:
            out.extend(self.bot_burst(record))
        return out

    def release_due(self, now_ts: float) -> list[dict[str, Any]]:
        due: list[dict[str, Any]] = []
        while self._held and self._held[0].release_at <= now_ts:
            due.append(heapq.heappop(self._held).record)
        return due

    def drain(self) -> list[dict[str, Any]]:
        rest = [h.record for h in sorted(self._held)]
        self._held.clear()
        return rest

    # -- synthetic bot traffic --------------------------------------------------------
    def bot_burst(self, seed_record: dict[str, Any]) -> list[dict[str, Any]]:
        n = self._rng.randint(50, 200)
        user_id = BOT_USER_ID_BASE + self._rng.randint(0, 99_999)
        session = str(uuid.uuid4())
        start: datetime = seed_record["event_time"]
        events = []
        for i in range(n):
            ts = start + timedelta(milliseconds=int(10_000 * i / n))
            events.append({
                **seed_record,
                "event_id": str(uuid.uuid4()),
                "event_time": ts,
                "original_event_time": ts,
                "event_type": "view",
                "user_id": user_id,
                "user_session": session,
                "chaos_tag": "bot",
            })
        return events


def utc_now_ts() -> float:
    return datetime.now(UTC).timestamp()
