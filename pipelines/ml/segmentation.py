"""Q6 — RFM + K-means customer segmentation (Spark MLlib) -> ml.user_segments.

Input  : gold.feat_user_offline (dbt, read by path) — buyers only (frequency > 0)
Model  : log1p(R,F,M) -> StandardScaler -> KMeans, k chosen by silhouette in [3..7]
Output : user_id, segment, segment_label, recency_days, frequency, monetary, churn_risk
Labels are derived from centroids so they stay meaningful when k / cluster ids change.
"""
from __future__ import annotations

import sys

import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import ML_USER_SEGMENTS, gold_path
from pipelines.ml.common import init_experiment

log = get_logger("segmentation")
EXPERIMENT = "customer_segmentation"
K_RANGE = range(3, 8)
CHURN_RECENCY_DAYS = 45


def label_segments(centers: list, k: int) -> dict[int, str]:
    """Rank clusters by a value score = -recency + frequency + monetary (standardized)."""
    scored = sorted(((-c[0] + c[1] + c[2], i) for i, c in enumerate(centers)), reverse=True)
    names = ["champions", "loyal", "potential", "needs_attention", "at_risk", "hibernating", "lost"]
    return {cluster: names[min(rank, len(names) - 1)] for rank, (_, cluster) in enumerate(scored)}


def main() -> int:
    spark = get_spark("ml_segmentation")
    init_experiment(EXPERIMENT)
    users = (spark.read.format("delta").load(gold_path("feat_user_offline"))
             .where("frequency > 0")
             .select("user_id", "recency_days", "frequency", "monetary")
             .withColumn("r", F.log1p("recency_days")).withColumn("f", F.log1p("frequency"))
             .withColumn("m", F.log1p("monetary"))
             .cache())
    n = users.count()
    log.info("training on %d buyers", n)

    assembler = VectorAssembler(inputCols=["r", "f", "m"], outputCol="raw")
    scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)
    evaluator = ClusteringEvaluator(featuresCol="features")

    best = None
    with mlflow.start_run(run_name="rfm_kmeans") as run:
        mlflow.log_param("n_users", n)
        for k in K_RANGE:
            model = Pipeline(stages=[assembler, scaler, KMeans(k=k, seed=42, featuresCol="features")]).fit(users)
            sil = evaluator.evaluate(model.transform(users))
            mlflow.log_metric("silhouette", sil, step=k)
            log.info("k=%d silhouette=%.4f", k, sil)
            if best is None or sil > best[1]:
                best = (k, sil, model)
        k, sil, model = best
        mlflow.log_params({"best_k": k})
        mlflow.log_metric("best_silhouette", sil)

        centers = [list(c) for c in model.stages[-1].clusterCenters()]
        labels = label_segments(centers, k)
        mapping = spark.createDataFrame(list(labels.items()), "segment int, segment_label string")
        out = (model.transform(users)
               .select("user_id", F.col("prediction").cast("int").alias("segment"),
                       "recency_days", "frequency", "monetary")
               .join(F.broadcast(mapping), "segment")
               .withColumn("churn_risk", F.col("recency_days") > CHURN_RECENCY_DAYS)
               .withColumn("model_run_id", F.lit(run.info.run_id))
               .withColumn("scored_at", F.current_timestamp()))
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ML_USER_SEGMENTS.path)
        sizes = {r.segment_label: r["count"] for r in out.groupBy("segment_label").count().collect()}
        mlflow.log_dict(sizes, "segment_sizes.json")
        log.info("segments written: %s", sizes)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
