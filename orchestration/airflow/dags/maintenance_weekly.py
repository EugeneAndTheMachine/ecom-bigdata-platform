"""Weekly VACUUM (retention 168h) on top of the daily OPTIMIZE. Sunday 05:00 UTC."""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG

from ecom_airflow.tasks import DEFAULT_ARGS, PROCESS_DATE, spark_task

with DAG(
    dag_id="maintenance_weekly",
    schedule="0 5 * * 0",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["maintenance"],
) as dag:
    spark_task("vacuum", "pipelines/batch/maintenance.py", ["--ds", PROCESS_DATE, "--vacuum"])
