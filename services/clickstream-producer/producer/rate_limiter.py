"""Pace emission so that the inter-event gaps of the file are reproduced at `speed`x."""
from __future__ import annotations

import time
from datetime import UTC, datetime


class ReplayPacer:
    def __init__(self, max_sleep_s: float = 5.0):
        self.max_sleep_s = max_sleep_s
        self.lag_seconds = 0.0  # >0 means we are behind schedule (producer too slow)

    def wait_until(self, scheduled: datetime) -> None:
        delta = (scheduled - datetime.now(UTC)).total_seconds()
        if delta > 0:
            time.sleep(min(delta, self.max_sleep_s))
            self.lag_seconds = 0.0
        else:
            self.lag_seconds = -delta
