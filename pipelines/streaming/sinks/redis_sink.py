"""Redis online-feature writers.

Key design (shared contract with services/recommend-api):
  trending:{category_l1}            ZSET product_id -> score   (latest 10-min window)
  trending:{category_l1}:w:{epoch}  ZSET per sliding window (TTL 20 min, source of the copy above)
  session:{user_session}            HASH live session features (TTL 1 h)
  bots:active                       SET  user_ids flagged in the last hour
  recs:{user_id}                    STRING JSON list of ALS candidates (written by F6 batch scoring)
"""
from __future__ import annotations

from collections.abc import Iterable

import redis

from ecom_common.config import Settings

SESSION_TTL_S = 3600
TRENDING_WINDOW_TTL_S = 1200
BOT_TTL_S = 3600


def client_from(host: str, port: int, password: str) -> redis.Redis:
    return redis.Redis(host=host, port=port, password=password or None, decode_responses=True,
                       socket_timeout=5, health_check_interval=30)


def get_client(s: Settings) -> redis.Redis:
    return client_from(s.serving.redis_host, s.serving.redis_port, s.serving.redis_password)


def serving_window_end(latest_end: int, window_s: int, slide_s: int) -> int:
    """Sliding windows that contain the newest event end anywhere in (t, t + window]. The one
    with the LARGEST end has only just opened (≈ one slide of data); the one that covers the
    full last `window` ends one slide after t, i.e. `window - slide` before the largest end."""
    return latest_end - (window_s - slide_s)


def write_trending(r: redis.Redis, rows: Iterable[tuple[str, int, int, float]],
                   window_s: int, slide_s: int) -> None:
    """rows: (category_l1, window_end_epoch, product_id, score). Update-mode values are FULL
    aggregates, so ZADD (set) is idempotent. Then the window covering the full last `window_s`
    per category becomes `trending:{cat}` via COPY ... REPLACE (atomic swap for readers)."""
    latest: dict[str, int] = {}
    pipe = r.pipeline(transaction=False)
    for cat, w_end, product_id, score in rows:
        key = f"trending:{cat}:w:{w_end}"
        pipe.zadd(key, {str(product_id): float(score)})
        pipe.expire(key, TRENDING_WINDOW_TTL_S)
        latest[cat] = max(latest.get(cat, 0), w_end)
    pipe.execute()
    pipe = r.pipeline(transaction=False)
    for cat, w_end in latest.items():
        # this window contains the newest event of `cat`, so it was written just above
        serving = serving_window_end(w_end, window_s, slide_s)
        pipe.copy(f"trending:{cat}:w:{serving}", f"trending:{cat}", replace=True)
        pipe.zadd("trending:categories", {cat: serving})
    pipe.execute()


def write_sessions(r: redis.Redis, rows: Iterable[dict]) -> None:
    pipe = r.pipeline(transaction=False)
    for n, row in enumerate(rows, start=1):
        key = f"session:{row['user_session']}"
        pipe.hset(key, mapping={k: ("" if v is None else str(v)) for k, v in row.items()})
        pipe.expire(key, SESSION_TTL_S)
        if n % 1000 == 0:
            pipe.execute()
    pipe.execute()


def flag_bots(r: redis.Redis, user_ids: Iterable[int]) -> None:
    ids = [str(u) for u in user_ids]
    if ids:
        r.sadd("bots:active", *ids)
        r.expire("bots:active", BOT_TTL_S)
