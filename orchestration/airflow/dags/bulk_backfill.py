"""Manual: load a REES46 monthly CSV straight into Bronze (history the stream never saw).

Trigger with conf {"input": "/data/raw/2019-Nov.csv.gz"}; afterwards run medallion_daily
for each backfilled day (Airflow UI -> Trigger with a logical date, or `airflow dags backfill`).
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG, Param

from ecom_airflow.tasks import DEFAULT_ARGS, python_module_task, spark_task

with DAG(
    dag_id="bulk_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    params={"input": Param("/data/raw/2019-Oct.csv.gz", type="string",
                           description="CSV path or glob inside the container (/data mount) or s3a://")},
    tags=["batch", "backfill"],
    doc_md=__doc__,
) as dag:
    load = spark_task("csv_to_bronze", "pipelines/batch/bulk_backfill.py",
                      ["--input", "{{ params.input }}"], executor_memory="4g")
    load >> python_module_task("register_tables", "pipelines.batch.register_tables")
