"""Task factories -> every DAG submits jobs the same way (one place to change resources/conf)."""
from __future__ import annotations

from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator

APP_ROOT = "/opt/app"
DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = f"{APP_ROOT}/pipelines/dbt"

# Airflow 3 cron schedules use CronTriggerTimetable: `ds` = the run date itself.
# The daily batch at 02:00 UTC therefore processes the PREVIOUS day.
PROCESS_DATE = "{{ macros.ds_add(ds, -1) }}"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

SPARK_CONF = {
    # Driver runs inside the Airflow worker (LocalExecutor -> scheduler container)
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executorEnv.PYTHONPATH": f"{APP_ROOT}/libs:{APP_ROOT}",
    "spark.sql.shuffle.partitions": "16",
}


def spark_task(task_id: str, app: str, args: list[str] | None = None, *,
               executor_memory: str = "2g", executor_cores: int = 2, **kwargs) -> SparkSubmitOperator:
    """`app` is relative to /opt/app, e.g. 'pipelines/batch/silver_events.py'."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{APP_ROOT}/{app}",
        application_args=args or [],
        name=f"airflow_{task_id}",
        conf=SPARK_CONF,
        executor_memory=executor_memory,
        executor_cores=executor_cores,
        total_executor_cores=executor_cores,
        driver_memory="1g",
        env_vars={"PYTHONPATH": f"{APP_ROOT}/libs:{APP_ROOT}"},
        **kwargs,
    )


def python_module_task(task_id: str, module: str, args: str = "", **kwargs) -> BashOperator:
    """Lightweight non-Spark job (e.g. Trino DDL) run with the worker's interpreter."""
    return BashOperator(task_id=task_id, bash_command=f"cd {APP_ROOT} && python -m {module} {args}", **kwargs)


def dbt_task(task_id: str, command: str, select: str | None = None, vars_: str | None = None,
             **kwargs) -> BashOperator:
    sel = f"--select {select}" if select else ""
    var = f"--vars '{vars_}'" if vars_ else ""
    return BashOperator(
        task_id=task_id,
        bash_command=(f"{DBT} {command} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
                      f"--target-path /tmp/dbt/target --log-path /tmp/dbt/logs {sel} {var}"),
        env={"DBT_PROCESS_DATE": PROCESS_DATE},
        append_env=True,
        **kwargs,
    )
