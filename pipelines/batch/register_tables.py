"""Register Spark-written Delta tables in the Trino catalog (`delta.<schema>.<table>`).

Spark writes by path (ADR-0003); Trino needs a metastore entry to query them. Registration
is a metadata-only operation and is idempotent here (existing tables are skipped). Run it
after any job that may have created a NEW table (schema evolution needs no re-registration:
Trino reads the schema from the Delta log).

    python -m pipelines.batch.register_tables
"""
from __future__ import annotations

import sys

import trino

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.tables import SPARK_OWNED_TABLES

log = get_logger("register_tables")
SCHEMAS = ("bronze", "silver", "gold", "ml")


def connect():
    s = get_settings()
    return trino.dbapi.connect(host=s.serving.trino_host, port=s.serving.trino_port,
                               user="register-tables", catalog=s.serving.trino_catalog)


def run(cur, sql: str):
    cur.execute(sql)
    return cur.fetchall()


def main() -> int:
    s = get_settings()
    root = s.s3.lakehouse_root
    cur = connect().cursor()
    for schema in SCHEMAS:
        run(cur, f"CREATE SCHEMA IF NOT EXISTS {schema} WITH (location = '{root}/{schema}')")

    for t in SPARK_OWNED_TABLES:
        if not t.register_in_trino:
            continue
        existing = {r[0] for r in run(cur, f"SHOW TABLES FROM {t.schema}")}
        if t.name in existing:
            continue
        try:
            run(cur, f"CALL system.register_table(schema_name => '{t.schema}', "
                     f"table_name => '{t.name}', table_location => '{t.path}')")
            log.info("registered %s -> %s", t.fqn, t.path)
        except trino.exceptions.TrinoUserError as exc:
            # typically: no _delta_log yet (job never ran) -> fine, try again next run
            log.warning("skip %s: %s", t.fqn, exc.message)
    return 0


if __name__ == "__main__":
    sys.exit(main())
