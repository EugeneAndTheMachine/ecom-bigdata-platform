"""Recommend API (Q7) — FastAPI.

GET /recommend/{user_id}?session_id=...&k=10
    1. pipelined Redis reads: ALS candidates, live session, bot flag, trending
    2. intent = GBT(session features)            (model loaded once from MLflow)
    3. re-rank, log impression to Kafka, return top-k
Latency budget p95 < 100 ms (measure with locust: tests/load/locustfile.py).
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from prometheus_client import Histogram
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import CONFIG
from app.impressions import ImpressionLogger
from app.model import IntentModel, session_features
from app.reranker import Weights, rerank
from app.store import FeatureStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
STAGE_LATENCY = Histogram("reco_stage_seconds", "Latency per stage", ["stage"],
                          buckets=(.001, .0025, .005, .01, .025, .05, .1, .25))
STATE: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    STATE["store"] = FeatureStore(CONFIG.redis_host, CONFIG.redis_port, CONFIG.redis_password)
    STATE["model"] = IntentModel(CONFIG.mlflow_tracking_uri, CONFIG.intent_model_uri)
    STATE["impressions"] = ImpressionLogger(CONFIG.kafka_bootstrap, CONFIG.impressions_topic)
    yield
    STATE["impressions"].close()
    await STATE["store"].close()


app = FastAPI(title="Recommend API", version="0.1.0", lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.get("/health")
async def health() -> dict:
    ok = await STATE["store"].ping()
    return {"status": "ok" if ok else "degraded", "intent_model": STATE["model"].version}


@app.get("/recommend/{user_id}")
async def recommend(user_id: int, session_id: str | None = None,
                    k: int = Query(CONFIG.default_k, ge=1, le=CONFIG.max_k)) -> dict:
    t0 = time.perf_counter()
    ctx = await STATE["store"].fetch(user_id, session_id)
    STAGE_LATENCY.labels("redis").observe(time.perf_counter() - t0)

    if ctx.is_bot:
        raise HTTPException(status_code=429, detail="traffic flagged as automated")

    t1 = time.perf_counter()
    feats = session_features(ctx.session) if ctx.session else {}
    intent = STATE["model"].predict(feats)
    items = rerank(ctx.candidates, ctx.trending, ctx.session.get("last_category"),
                   ctx.product_category, intent, k,
                   Weights(CONFIG.w_als, CONFIG.w_category, CONFIG.w_trending))
    STAGE_LATENCY.labels("rerank").observe(time.perf_counter() - t1)

    strategy = "als+session" if ctx.candidates else "cold_start_trending"
    STATE["impressions"].log({"user_id": user_id, "session_id": session_id, "strategy": strategy,
                              "intent": intent, "items": [i["product_id"] for i in items],
                              "model_version": STATE["model"].version})
    return {"user_id": user_id, "session_id": session_id, "strategy": strategy,
            "intent_score": round(intent, 4), "items": items,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}
