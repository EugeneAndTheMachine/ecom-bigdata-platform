"""DAG import test (Airflow 3.3 API). Locally:
    AIRFLOW__CORE__LOAD_EXAMPLES=false PYTHONPATH=libs:.:orchestration/airflow pytest orchestration/airflow/tests
In the container:
    docker compose exec airflow-scheduler pytest /opt/app/orchestration/airflow/tests -q
"""
from pathlib import Path

import pytest

DagBag = pytest.importorskip("airflow.models").DagBag
DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED = {"medallion_daily", "ml_weekly", "maintenance_weekly", "bulk_backfill"}


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(bag):
    assert not bag.import_errors, bag.import_errors
    assert set(bag.dag_ids) >= EXPECTED


def test_medallion_order(bag):
    dag = bag.dags["medallion_daily"]
    downstream = dag.get_task("bronze_load").get_flat_relative_ids(upstream=False)
    assert {"silver.silver_events", "gold.dbt_build", "reconciliation"} <= downstream
    assert dag.get_task("silver.silver_products_scd2").depends_on_past
    assert dag.max_active_runs == 1


def test_spark_tasks_process_previous_day(bag):
    task = bag.dags["medallion_daily"].get_task("silver.silver_events")
    assert task.application_args == ["--ds", "{{ macros.ds_add(ds, -1) }}"]
