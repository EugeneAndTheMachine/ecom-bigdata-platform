"""Q7 — Implicit-feedback ALS recommender (Spark MLlib) with offline evaluation.

ratings(user, product) = log1p( Σ weight(event) ), weight: view 1, cart 3, purchase 5
Split  : last HOLDOUT_DAYS of history = test (carts + purchases are the relevant items)
Metrics: Recall@10, NDCG@10 vs. the "most popular" baseline
Champion: model saved to s3a://lakehouse/ml/models/als/<run_id>; copied to .../champion when
NDCG@10 beats the previous champion (tag champion=true in MLflow).
"""
from __future__ import annotations

import math
import sys

import mlflow
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.transforms import event_weight
from pipelines.ml.common import init_experiment, latest_champion_run, ml_events

log = get_logger("als")
EXPERIMENT = "als_recommender"
K = 10
HOLDOUT_DAYS = 7
HISTORY_DAYS = 90
PARAMS = {"rank": 32, "regParam": 0.1, "alpha": 10.0, "maxIter": 10}


def model_root() -> str:
    return f"{get_settings().s3.lakehouse_root}/ml/models/als"


def ratings(events: DataFrame) -> DataFrame:
    return (events.groupBy("user_id", "product_id")
            .agg(F.log1p(F.sum(event_weight())).alias("rating"))
            .where("rating > 0")
            .select(F.col("user_id").cast("int"), F.col("product_id").cast("int"), "rating"))


def ranking_metrics(recs: DataFrame, truth: DataFrame, k: int = K) -> dict[str, float]:
    """recs: user_id, product_id, rank (1..k) ; truth: user_id, product_id (relevant items)."""
    hits = recs.join(truth.withColumn("rel", F.lit(1)), ["user_id", "product_id"], "left").fillna(0, ["rel"])
    per_user = hits.groupBy("user_id").agg(
        F.sum("rel").alias("hits"),
        F.sum(F.col("rel") / F.log2(F.col("rank") + 1)).alias("dcg"))
    n_rel = truth.groupBy("user_id").agg(F.count(F.lit(1)).alias("n_rel"))
    idcg_terms = F.aggregate(F.sequence(F.lit(1), F.least(F.col("n_rel"), F.lit(k))),
                             F.lit(0.0), lambda acc, i: acc + 1 / F.log2(i + 1))
    m = (n_rel.join(per_user, "user_id", "left").fillna(0, ["hits", "dcg"])
         .withColumn("recall", F.col("hits") / F.least(F.col("n_rel"), F.lit(k)))
         .withColumn("ndcg", F.col("dcg") / idcg_terms))
    row = m.agg(F.avg("recall").alias("recall"), F.avg("ndcg").alias("ndcg")).first()
    return {f"recall_at_{k}": float(row.recall or 0), f"ndcg_at_{k}": float(row.ndcg or 0)}


def main() -> int:
    spark = get_spark("ml_als_train")
    init_experiment(EXPERIMENT)
    ev, max_date = ml_events(spark)
    cutoff = F.date_sub(F.lit(max_date), HOLDOUT_DAYS)
    history = ev.where(F.col("event_date") > F.date_sub(F.lit(max_date), HISTORY_DAYS))
    train_ev = history.where(F.col("event_date") <= cutoff)
    test_ev = history.where((F.col("event_date") > cutoff) & F.col("event_type").isin("cart", "purchase"))

    train = ratings(train_ev).cache()
    truth = (test_ev.select(F.col("user_id").cast("int"), F.col("product_id").cast("int")).distinct()
             .join(train.select("user_id").distinct(), "user_id", "left_semi")   # warm users only
             .cache())

    with mlflow.start_run(run_name="als_implicit") as run:
        als = ALS(userCol="user_id", itemCol="product_id", ratingCol="rating", implicitPrefs=True,
                  coldStartStrategy="drop", nonnegative=True, seed=42, **PARAMS)
        model = als.fit(train)
        users = truth.select("user_id").distinct()
        recs = (model.recommendForUserSubset(users, K)
                .select("user_id", F.posexplode("recommendations").alias("pos", "rec"))
                .select("user_id", F.col("rec.product_id").alias("product_id"), (F.col("pos") + 1).alias("rank")))
        metrics = ranking_metrics(recs, truth)

        popular = (train.groupBy("product_id").agg(F.sum("rating").alias("s"))
                   .orderBy(F.desc("s")).limit(K)
                   .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("s")))).drop("s"))
        baseline = ranking_metrics(users.crossJoin(popular), truth)
        mlflow.log_params({**PARAMS, "history_days": HISTORY_DAYS, "holdout_days": HOLDOUT_DAYS})
        mlflow.log_metrics({**metrics, **{f"baseline_{k}": v for k, v in baseline.items()},
                            "n_train_ratings": train.count(), "n_eval_users": users.count()})

        path = f"{model_root()}/{run.info.run_id}"
        model.write().overwrite().save(path)
        mlflow.log_param("model_path", path)

        _, champ_ndcg, _ = latest_champion_run(EXPERIMENT, f"ndcg_at_{K}")
        ndcg = metrics[f"ndcg_at_{K}"]
        if champ_ndcg is None or (not math.isnan(champ_ndcg) and ndcg > champ_ndcg):
            model.write().overwrite().save(f"{model_root()}/champion")
            mlflow.set_tag("champion", "true")
            log.info("new ALS champion ndcg@%d=%.4f (prev=%s)", K, ndcg, champ_ndcg)
        log.info("ALS %s | baseline %s", metrics, baseline)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
