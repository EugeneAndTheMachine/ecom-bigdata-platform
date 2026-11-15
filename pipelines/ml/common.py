"""MLflow helpers (experiment setup, champion/challenger promotion) and the training timeline."""
from __future__ import annotations

from datetime import date

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.tables import SILVER_EVENTS

log = get_logger("ml")
CHAMPION = "champion"


def ml_events(spark: SparkSession) -> tuple[DataFrame, date]:
    """Human Silver events of ONE timeline and its last event_date (the "as of" anchor).

    Replay rows (_source=kafka_connect) carry "now" timestamps, bulk rows (bulk_csv) keep
    2019-10..2020-04; anchoring "last N days" on the max over both would only ever see replay.
    Same rule as the dbt macro ml_source_predicate (ML_SOURCE: auto | bulk_csv | kafka_connect | all).
    """
    ev = spark.read.format("delta").load(SILVER_EVENTS.path).where("NOT is_bot")
    source = get_settings().ml_source
    if source == "auto":
        source = "bulk_csv" if ev.where(F.col("_source") == "bulk_csv").limit(1).count() else "kafka_connect"
    if source != "all":
        ev = ev.where(F.col("_source") == source)
    as_of = ev.agg(F.max("event_date")).first()[0]
    log.info("ML timeline source=%s as_of=%s", source, as_of)
    return ev, as_of


def init_experiment(name: str) -> None:
    mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
    mlflow.set_experiment(name)


def champion_metric(model_name: str, metric: str) -> float | None:
    """Metric of the model version currently aliased `champion` (None if no champion yet)."""
    client = MlflowClient()
    try:
        mv = client.get_model_version_by_alias(model_name, CHAMPION)
    except MlflowException:
        return None
    return client.get_run(mv.run_id).data.metrics.get(metric)


def promote_if_better(model_name: str, version: str, metric: str, value: float,
                      higher_is_better: bool = True, min_delta: float = 0.0) -> bool:
    """Challenger becomes champion only if it beats the current champion by min_delta."""
    current = champion_metric(model_name, metric)
    better = current is None or (
        value > current + min_delta if higher_is_better else value < current - min_delta)
    if better:
        MlflowClient().set_registered_model_alias(model_name, CHAMPION, version)
    log.info("%s v%s %s=%.4f champion=%s -> promoted=%s", model_name, version, metric, value, current, better)
    return better


def latest_champion_run(experiment: str, metric: str) -> tuple[str | None, float | None, str | None]:
    """For models kept outside the registry (Spark ALS): run tagged champion=true."""
    runs = mlflow.search_runs(experiment_names=[experiment], filter_string="tags.champion = 'true'",
                              order_by=["attributes.start_time DESC"], max_results=1)
    if runs.empty:
        return None, None, None
    row = runs.iloc[0]
    return row["run_id"], row.get(f"metrics.{metric}"), row.get("params.model_path")
