"""MLflow helpers: experiment setup and champion/challenger promotion."""
from __future__ import annotations

import mlflow
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from ecom_common.config import get_settings
from ecom_common.log import get_logger

log = get_logger("ml")
CHAMPION = "champion"


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
