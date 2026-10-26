"""Cold path, daily @ 02:00 UTC, processes D-1.

landing (Kafka Connect parquet) -> bronze -> register -> silver (events, sessions, products SCD2)
-> register -> dbt build (gold + tests) -> maintenance (OPTIMIZE/ZORDER) -> reconciliation
"""
from __future__ import annotations

import pendulum
from airflow.sdk import DAG, TaskGroup

from ecom_airflow.assets import GOLD_MARTS, SILVER_EVENTS
from ecom_airflow.tasks import DEFAULT_ARGS, PROCESS_DATE, dbt_task, python_module_task, spark_task

DS = ["--ds", PROCESS_DATE]

with DAG(
    dag_id="medallion_daily",
    description="Bronze -> Silver -> Gold for REES46 clickstream (D-1)",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,          # MERGE/replaceWhere are idempotent but not concurrent-safe
    default_args=DEFAULT_ARGS,
    tags=["batch", "medallion"],
    doc_md=__doc__,
) as dag:
    bronze = spark_task("bronze_load", "pipelines/batch/bronze_load.py", DS)
    register_bronze = python_module_task("register_bronze", "pipelines.batch.register_tables")

    with TaskGroup("silver") as silver:
        events = spark_task("silver_events", "pipelines/batch/silver_events.py", DS,
                            outlets=[SILVER_EVENTS], executor_memory="3g")
        sessions = spark_task("silver_sessions", "pipelines/batch/silver_sessions.py", DS)
        # SCD2 must be applied day after day in order
        products = spark_task("silver_products_scd2", "pipelines/batch/silver_products_scd2.py", DS,
                              depends_on_past=True, wait_for_downstream=False)
        events >> [sessions, products]

    register_silver = python_module_task("register_silver", "pipelines.batch.register_tables")

    with TaskGroup("gold") as gold:
        seed = dbt_task("dbt_seed", "seed")
        # staging + marts + features (+ all tests); incremental models self-heal via lookback_days
        build = dbt_task("dbt_build", "build", outlets=[GOLD_MARTS])
        seed >> build

    maintenance = spark_task("maintenance", "pipelines/batch/maintenance.py", DS)
    reconciliation = spark_task("reconciliation", "pipelines/batch/reconciliation.py",
                                DS + ["--warn-above", "0.02", "--fail-above", "0.10"])

    bronze >> register_bronze >> silver >> register_silver >> gold
    silver >> reconciliation          # compares Silver vs ClickHouse; independent of dbt
    gold >> maintenance
