"""Helpers shared by batch jobs: CLI date args, Delta existence, idempotent writes."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from ecom_common.tables import Table


def parse_ds(argv: list[str] | None = None, extra: callable | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, help="logical date YYYY-MM-DD (Airflow {{ ds }})")
    if extra:
        extra(p)
    ns = p.parse_args(argv)
    ns.ds_date = datetime.strptime(ns.ds, "%Y-%m-%d").date()
    return ns


def day_range(ds: date, before: int = 0, after: int = 0) -> list[str]:
    return [(ds + timedelta(days=d)).isoformat() for d in range(-before, after + 1)]


def delta_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def overwrite_partition(df: DataFrame, table: Table, predicate: str) -> None:
    """Idempotent re-run: atomically replace only the rows matching `predicate`."""
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", predicate)
       .partitionBy(*table.partition_by)
       .save(table.path))


def append(df: DataFrame, table: Table) -> None:
    df.write.format("delta").mode("append").partitionBy(*table.partition_by).save(table.path)
