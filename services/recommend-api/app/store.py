"""Redis access for the online path: 2 pipelined round-trips per request.

Contract (written by Spark):
  recs:{user_id}          JSON [{"p": product_id, "s": score}, ...]     (ml/batch_scoring.py)
  session:{user_session}  HASH live features                           (streaming/session_state.py)
  trending:{category_l1}  ZSET product_id -> score                      (streaming/trending.py)
  trending:categories     ZSET category_l1 -> last window_end epoch
  product:category        HASH product_id -> category_l1
  bots:active             SET user_ids flagged by bot detection
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import redis.asyncio as aioredis


@dataclass
class Context:
    candidates: list[tuple[int, float]] = field(default_factory=list)
    session: dict[str, str] = field(default_factory=dict)
    is_bot: bool = False
    trending: list[tuple[int, float]] = field(default_factory=list)
    product_category: dict[int, str] = field(default_factory=dict)


class FeatureStore:
    def __init__(self, host: str, port: int, password: str):
        self.r = aioredis.Redis(host=host, port=port, password=password or None, decode_responses=True)

    async def ping(self) -> bool:
        return bool(await self.r.ping())

    async def close(self) -> None:
        await self.r.aclose()

    async def fetch(self, user_id: int, session_id: str | None, trending_n: int = 50) -> Context:
        ctx = Context()
        async with self.r.pipeline(transaction=False) as p:
            p.get(f"recs:{user_id}")
            p.hgetall(f"session:{session_id}") if session_id else p.echo("")
            p.sismember("bots:active", str(user_id))
            p.zrevrange("trending:categories", 0, 2)
            recs_raw, session, is_bot, top_cats = await p.execute()

        ctx.candidates = [(int(x["p"]), float(x["s"])) for x in json.loads(recs_raw)] if recs_raw else []
        ctx.session = session if isinstance(session, dict) else {}
        ctx.is_bot = bool(is_bot)

        cats = [ctx.session["last_category"]] if ctx.session.get("last_category") else list(top_cats or [])
        async with self.r.pipeline(transaction=False) as p:
            for cat in cats:
                p.zrevrange(f"trending:{cat}", 0, trending_n - 1, withscores=True)
            ids = [str(pid) for pid, _ in ctx.candidates]
            if ids:
                p.hmget("product:category", ids)
            results = await p.execute()

        trending: dict[int, float] = {}
        for res in results[: len(cats)]:
            for pid, score in res:
                trending[int(pid)] = max(trending.get(int(pid), 0.0), float(score))
        ctx.trending = sorted(trending.items(), key=lambda t: t[1], reverse=True)
        if ctx.candidates:
            ctx.product_category = {pid: c for (pid, _), c in zip(ctx.candidates, results[-1], strict=True) if c}
        for cat in cats:  # trending items' category is known by construction
            for pid, _ in ctx.trending:
                ctx.product_category.setdefault(pid, cat)
        return ctx
