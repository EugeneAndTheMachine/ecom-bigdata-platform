"""Q6/Q7 — Purchase-intent model: will a session with a cart end in a purchase?

* Features: libs/ecom_common/features.py::INTENT_FEATURES (shared with the online API)
* Split   : by time (train on older sessions, test on the newest 20 %) -> no future leakage
* Model   : scikit-learn HistGradientBoosting (a GBT; light enough to serve in FastAPI
            with sub-millisecond inference — a Spark GBT model would require Spark at serving)
* Baseline: logistic regression; both logged to MLflow
* Registry: registered as `purchase_intent`; alias `champion` moves only if AUC improves.
"""
from __future__ import annotations

import sys

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ecom_common.features import INTENT_FEATURES, INTENT_LABEL
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import gold_path
from pipelines.ml.common import init_experiment, promote_if_better

log = get_logger("purchase_intent")
EXPERIMENT = "purchase_intent"
MODEL_NAME = "purchase_intent"
MAX_ROWS = 2_000_000
TEST_FRACTION = 0.2


def load_training_frame() -> pd.DataFrame:
    spark = get_spark("ml_purchase_intent_extract")
    df = spark.read.format("delta").load(gold_path("feat_session_training"))
    total = df.count()
    frac = min(1.0, MAX_ROWS / max(total, 1))
    pdf = (df.sample(fraction=frac, seed=42) if frac < 1 else df) \
        .select("session_start", *INTENT_FEATURES, INTENT_LABEL).toPandas()
    spark.stop()
    return pdf.sort_values("session_start").reset_index(drop=True)


def time_split(pdf: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cut = int(len(pdf) * (1 - TEST_FRACTION))
    return pdf.iloc[:cut], pdf.iloc[cut:]


def best_f1(y_true: np.ndarray, proba: np.ndarray) -> tuple[float, float]:
    p, r, t = precision_recall_curve(y_true, proba)
    f1 = 2 * p * r / np.clip(p + r, 1e-9, None)
    i = int(np.nanargmax(f1[:-1])) if len(t) else 0
    return float(f1[i]), float(t[i]) if len(t) else 0.5


def main() -> int:
    init_experiment(EXPERIMENT)
    pdf = load_training_frame()
    train, test = time_split(pdf)
    X_tr, y_tr = train[INTENT_FEATURES], train[INTENT_LABEL].astype(int)
    X_te, y_te = test[INTENT_FEATURES], test[INTENT_LABEL].astype(int)
    log.info("train=%d test=%d positive_rate=%.3f", len(train), len(test), y_tr.mean())

    with mlflow.start_run(run_name="baseline_logreg"):
        base = make_pipeline(StandardScaler(), LogisticRegression(max_iter=500)).fit(X_tr, y_tr)
        mlflow.log_metric("auc", roc_auc_score(y_te, base.predict_proba(X_te)[:, 1]))

    with mlflow.start_run(run_name="hist_gbt") as run:
        params = {"max_iter": 300, "learning_rate": 0.08, "max_leaf_nodes": 31,
                  "l2_regularization": 1.0, "early_stopping": True, "random_state": 42}
        model = HistGradientBoostingClassifier(**params).fit(X_tr, y_tr)
        proba = model.predict_proba(X_te)[:, 1]
        auc = roc_auc_score(y_te, proba)
        f1_at_best, threshold = best_f1(y_te.to_numpy(), proba)
        mlflow.log_params({**params, "features": ",".join(INTENT_FEATURES), "n_train": len(train)})
        mlflow.log_metrics({"auc": auc, "f1_best": f1_at_best, "f1_at_0_5": f1_score(y_te, proba >= 0.5),
                            "best_threshold": threshold})
        info = mlflow.sklearn.log_model(
            model, artifact_path="model", registered_model_name=MODEL_NAME,
            signature=infer_signature(X_te.head(100), proba[:100]), input_example=X_te.head(5))
        version = info.registered_model_version
        promote_if_better(MODEL_NAME, str(version), "auc", auc, min_delta=0.002)
        log.info("run=%s auc=%.4f f1=%.4f thr=%.3f", run.info.run_id, auc, f1_at_best, threshold)
    return 0


if __name__ == "__main__":
    sys.exit(main())
