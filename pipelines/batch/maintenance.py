"""Delta table maintenance: compaction (OPTIMIZE + Z-ORDER) and VACUUM.

Streaming and hourly loads leave many small Parquet files; Z-ORDER co-locates rows by the
columns most used in filters/joins so data skipping works.

    spark-submit pipelines/batch/maintenance.py --ds 2026-10-26            # optimize ds partitions
    spark-submit pipelines/batch/maintenance.py --ds 2026-10-26 --vacuum   # + vacuum (weekly)
"""
from __future__ import annotations

import sys

from delta.tables import DeltaTable

from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import BRONZE_CLICKSTREAM, SILVER_EVENTS, SILVER_PRODUCTS_SCD2, SILVER_SESSIONS, Table
from pipelines.batch.utils import delta_exists, parse_ds

log = get_logger("maintenance")

# table -> (partition column used to scope OPTIMIZE, z-order columns)
PLAN: list[tuple[Table, str | None, list[str]]] = [
    (BRONZE_CLICKSTREAM, "ingest_date", ["user_id"]),
    (SILVER_EVENTS, "event_date", ["user_id", "product_id"]),
    (SILVER_SESSIONS, "session_date", ["user_id"]),
    (SILVER_PRODUCTS_SCD2, None, ["product_id"]),
]
VACUUM_RETENTION_HOURS = 168  # keep 7 days of time travel


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv, extra=lambda p: p.add_argument("--vacuum", action="store_true"))
    spark = get_spark(f"maintenance_{args.ds}")
    for table, part_col, zcols in PLAN:
        if not delta_exists(spark, table.path):
            continue
        dt = DeltaTable.forPath(spark, table.path)
        builder = dt.optimize()
        if part_col:
            builder = builder.where(f"{part_col} = '{args.ds}'")
        metrics = builder.executeZOrderBy(*zcols)
        log.info("OPTIMIZE %s zorder=%s -> %s", table.fqn, zcols, metrics.select("metrics.numFilesRemoved",
                                                                                  "metrics.numFilesAdded").first())
        if args.vacuum:
            dt.vacuum(VACUUM_RETENTION_HOURS)
            log.info("VACUUM %s retain=%dh", table.fqn, VACUUM_RETENTION_HOURS)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
