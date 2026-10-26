from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiConfig:
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT_INTERNAL", "6379"))
    redis_password: str = os.getenv("REDIS_PASSWORD", "")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    impressions_topic: str = os.getenv("TOPIC_RECO_IMPRESSIONS", "reco.impressions")
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    intent_model_uri: str = os.getenv("INTENT_MODEL_URI", "models:/purchase_intent@champion")
    default_k: int = int(os.getenv("API_DEFAULT_K", "10"))
    max_k: int = 50
    # blending weights (tune with offline replay / online A/B test)
    w_als: float = 0.6
    w_category: float = 0.3
    w_trending: float = 0.1


CONFIG = ApiConfig()
