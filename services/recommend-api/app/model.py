"""Purchase-intent model loaded ONCE at start-up from the MLflow registry alias `champion`.
If the registry is unreachable / empty the API degrades gracefully (intent = 0.5)."""
from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime

import pandas as pd

log = logging.getLogger("intent-model")

# Must mirror libs/ecom_common/features.py::INTENT_FEATURES (the API image does not ship libs/)
INTENT_FEATURES = ["n_views", "n_carts", "cart_items", "cart_value", "n_categories",
                   "duration_s", "hour_of_day", "day_of_week"]


def session_features(session: dict[str, str], now: datetime | None = None) -> dict[str, float]:
    """Build the online feature vector from Redis `session:{id}` (see session_state.py)."""
    def f(key: str) -> float:
        try:
            return float(session.get(key) or 0)
        except ValueError:
            return 0.0

    start = session.get("first_event_time")
    try:
        start_dt = pd.Timestamp(start).to_pydatetime() if start else (now or datetime.now(UTC))
    except ValueError:
        start_dt = now or datetime.now(UTC)
    cats = [c for c in (session.get("recent_categories") or "").split("|") if c]
    return {
        "n_views": f("n_views"),
        "n_carts": f("n_carts"),
        "cart_items": f("cart_items"),
        "cart_value": f("cart_value"),
        "n_categories": float(min(len(cats), 5)),
        "duration_s": f("duration_s"),
        "hour_of_day": float(start_dt.hour),
        "day_of_week": float(start_dt.isoweekday()),
    }


class IntentModel:
    def __init__(self, tracking_uri: str, model_uri: str):
        self.model = None
        self.version = "none"
        # fail fast: never let a down registry block API start-up for minutes (default retries/backoff)
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")
        try:
            import mlflow

            mlflow.set_tracking_uri(tracking_uri)
            t0 = time.perf_counter()
            self.model = mlflow.pyfunc.load_model(model_uri)
            self.version = getattr(self.model.metadata, "run_id", "unknown")
            log.info("intent model %s loaded in %.2fs", model_uri, time.perf_counter() - t0)
        except Exception as exc:  # noqa: BLE001 — degrade, never crash the API
            log.warning("intent model unavailable (%s): using prior 0.5", exc)

    def predict(self, features: dict[str, float]) -> float:
        if self.model is None or not features:
            return 0.5
        x = pd.DataFrame([[features[c] for c in INTENT_FEATURES]], columns=INTENT_FEATURES)
        raw = self.model._model_impl  # sklearn estimator behind the pyfunc wrapper
        try:
            return float(raw.predict_proba(x)[0, 1])
        except AttributeError:
            return float(self.model.predict(x)[0])
