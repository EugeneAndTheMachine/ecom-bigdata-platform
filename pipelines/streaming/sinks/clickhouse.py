"""ClickHouse sink used inside foreachBatch.

Micro-batch outputs of the hot path are small (aggregates), so they are collected to the
driver and inserted with clickhouse-connect. Idempotency comes from the target tables:
ReplacingMergeTree(version) + ORDER BY the business key -> a replayed batch overwrites itself.
"""
from __future__ import annotations

import threading
from datetime import UTC, datetime

import pandas as pd

from ecom_common.config import Settings

# One client PER THREAD: in dev all streaming queries share one driver and Spark runs each
# query's foreachBatch on its own thread. A clickhouse-connect client holds one HTTP session,
# and concurrent requests on the same session fail ("concurrent queries within the same session").
_LOCAL = threading.local()


def _client(s: Settings):
    import clickhouse_connect  # imported lazily: only drivers need it

    clients: dict[str, object] = _LOCAL.__dict__.setdefault("clients", {})
    key = f"{s.serving.clickhouse_host}:{s.serving.clickhouse_port}"
    if key not in clients:
        clients[key] = clickhouse_connect.get_client(
            host=s.serving.clickhouse_host,
            port=s.serving.clickhouse_port,
            username=s.serving.clickhouse_user,
            password=s.serving.clickhouse_password,
            database=s.serving.clickhouse_db,
        )
    return clients[key]


def insert_df(s: Settings, table: str, pdf: pd.DataFrame, version_col: str | None = "updated_at") -> int:
    if pdf.empty:
        return 0
    if version_col and version_col not in pdf.columns:
        pdf = pdf.assign(**{version_col: pd.Timestamp(datetime.now(UTC)).tz_localize(None)})
    _client(s).insert_df(table, pdf, database=s.serving.clickhouse_db)
    return len(pdf)


def query_df(s: Settings, sql: str) -> pd.DataFrame:
    return _client(s).query_df(sql)
