"""Lakehouse table registry.

Spark reads/writes Delta tables BY PATH (no Spark <-> Hive Metastore coupling, see ADR-0003).
Trino registers the same paths in HMS (`delta.<schema>.<table>`) so dbt / Superset can query them.
Every job must resolve locations through this module — never hard-code paths.
"""
from __future__ import annotations

from dataclasses import dataclass

from ecom_common.config import get_settings


@dataclass(frozen=True)
class Table:
    schema: str
    name: str
    partition_by: tuple[str, ...] = ()
    register_in_trino: bool = True

    @property
    def path(self) -> str:
        return f"{get_settings().s3.lakehouse_root}/{self.schema}/{self.name}"

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


def landing_path(topic: str) -> str:
    """Raw files written by Kafka Connect S3 sink (immutable)."""
    return f"{get_settings().s3.lakehouse_root}/landing/topics/{topic}"


def checkpoint_path(job: str) -> str:
    return f"{get_settings().s3.checkpoint_root}/{job}"


def quarantine_path(table: Table) -> str:
    return f"{get_settings().s3.lakehouse_root}/quarantine/{table.schema}/{table.name}"


# --- Bronze: raw, append-only, partitioned by ingest_date ------------------------------
BRONZE_CLICKSTREAM = Table("bronze", "clickstream", ("ingest_date",))
BRONZE_ALERTS_BOT = Table("bronze", "alerts_bot", ("ingest_date",))
BRONZE_ALERTS_CART = Table("bronze", "alerts_cart_abandon", ("ingest_date",))

# --- Silver: clean, conformed, deduplicated --------------------------------------------
SILVER_EVENTS = Table("silver", "events", ("event_date",))
SILVER_SESSIONS = Table("silver", "sessions", ("session_date",))
SILVER_PRODUCTS_SCD2 = Table("silver", "products_scd2")
SILVER_BOT_USERS = Table("silver", "bot_users", ("event_date",))
SILVER_DQ_RESULTS = Table("silver", "dq_results", ("run_date",))

# --- ML outputs (Gold itself is built by dbt via Trino in schema `gold`) ---------------
ML_USER_SEGMENTS = Table("ml", "user_segments")
ML_USER_RECS = Table("ml", "user_recs")
GOLD_RECONCILIATION = Table("gold", "mart_reconciliation", ("check_date",))

SPARK_OWNED_TABLES: tuple[Table, ...] = (
    BRONZE_CLICKSTREAM,
    BRONZE_ALERTS_BOT,
    BRONZE_ALERTS_CART,
    SILVER_EVENTS,
    SILVER_SESSIONS,
    SILVER_PRODUCTS_SCD2,
    SILVER_BOT_USERS,
    SILVER_DQ_RESULTS,
    ML_USER_SEGMENTS,
    ML_USER_RECS,
    GOLD_RECONCILIATION,
)


def gold_path(model: str) -> str:
    """Location dbt models are written to (see pipelines/dbt/dbt_project.yml +location)."""
    return f"{get_settings().s3.lakehouse_root}/gold/{model}"
