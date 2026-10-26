"""ML training, weekly (Monday 04:00 UTC) and after the gold layer is rebuilt.

segmentation (RFM + K-means) -> dbt dim_user+ (picks up the new segments)
purchase_intent (HistGradientBoosting, MLflow alias champion)
ALS -> batch_scoring (Delta ml.user_recs + Redis recs:{user_id})
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG

from ecom_airflow.assets import ML_RECS
from ecom_airflow.tasks import DEFAULT_ARGS, dbt_task, python_module_task, spark_task

with DAG(
    dag_id="ml_weekly",
    schedule="0 4 * * 1",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ml"],
    doc_md=__doc__,
) as dag:
    segmentation = spark_task("segmentation", "pipelines/ml/segmentation.py", executor_memory="3g")
    register_ml = python_module_task("register_ml_tables", "pipelines.batch.register_tables")
    refresh_dim_user = dbt_task("dbt_dim_user", "build", select="dim_user+")

    # independent branch; the API picks up the new champion on restart
    spark_task("purchase_intent", "pipelines/ml/purchase_intent.py")
    als = spark_task("als_recommender", "pipelines/ml/als_recommender.py", executor_memory="4g")
    scoring = spark_task("batch_scoring", "pipelines/ml/batch_scoring.py", outlets=[ML_RECS])

    segmentation >> register_ml >> refresh_dim_user
    als >> scoring >> python_module_task("register_recs", "pipelines.batch.register_tables")
