"""Nightly batch-vs-stream reconciliation (the quantitative proof of the Lambda trade-off).

For logical date `ds`, per hour:
  batch  = silver.events (only rows that went through Kafka: _source = 'kafka_connect';
           bots INCLUDED because the speed layer does not filter them)
  stream = ClickHouse rt_funnel_1m (views/carts/purchases/revenue) and rt_purchases
Writes gold.mart_reconciliation(check_date, hour, metric, batch_value, stream_value,
abs_diff, diff_pct, late_events, status). Exit code 2 if any metric exceeds --fail-above.

Expected root causes of a gap: events behind the watermark (arrived_late), duplicates
outside the dedup state, consumer restarts without checkpoint, bugs.
"""
from __future__ import annotations

import sys

import pandas as pd
from pyspark.sql import functions as F

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import GOLD_RECONCILIATION, SILVER_EVENTS
from ecom_common.transforms import funnel_aggs
from pipelines.batch.utils import overwrite_partition, parse_ds
from pipelines.streaming.sinks.clickhouse import query_df

log = get_logger("reconciliation")
METRICS = ["views", "carts", "purchases", "revenue"]


def batch_side(spark, ds: str) -> pd.DataFrame:
    ev = (spark.read.format("delta").load(SILVER_EVENTS.path)
          .where((F.col("event_date") == ds) & (F.col("_source") == "kafka_connect")))
    agg = ev.groupBy(F.col("event_hour").alias("hour")).agg(
        *funnel_aggs(), F.sum(F.col("arrived_late").cast("int")).alias("late_events"))
    return agg.toPandas()


def stream_side(s, ds: str) -> pd.DataFrame:
    funnel = query_df(s, f"""
        SELECT toHour(window_start) AS hour,
               sum(views) AS views, sum(carts) AS carts, sum(purchases) AS purchases, sum(revenue) AS revenue
        FROM {s.serving.clickhouse_db}.rt_funnel_1m FINAL
        WHERE toDate(window_start) = '{ds}' GROUP BY hour""")
    purchases = query_df(s, f"""
        SELECT toHour(event_time) AS hour, count() AS enriched_purchases
        FROM {s.serving.clickhouse_db}.rt_purchases FINAL
        WHERE toDate(event_time) = '{ds}' GROUP BY hour""")
    return funnel.merge(purchases, on="hour", how="outer")


def compare(batch: pd.DataFrame, stream: pd.DataFrame, ds: str, warn_pct: float) -> pd.DataFrame:
    m = batch.merge(stream, on="hour", how="outer", suffixes=("_batch", "_stream")).fillna(0)
    rows = []
    for _, r in m.iterrows():
        pairs = {k: (r[f"{k}_batch"], r[f"{k}_stream"]) for k in METRICS}
        pairs["purchases_enriched"] = (r["purchases_batch"], r.get("enriched_purchases", 0))
        for metric, (b, st) in pairs.items():
            diff = float(b) - float(st)
            pct = abs(diff) / float(b) if b else (0.0 if not st else 1.0)
            rows.append({
                "check_date": pd.to_datetime(ds).date(), "hour": int(r["hour"]), "metric": metric,
                "batch_value": float(b), "stream_value": float(st), "abs_diff": diff, "diff_pct": pct,
                "late_events": int(r.get("late_events", 0)),
                "status": "ok" if pct <= warn_pct else "warn",
            })
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    args = parse_ds(argv, extra=lambda p: (p.add_argument("--warn-above", type=float, default=0.02),
                                           p.add_argument("--fail-above", type=float, default=1.0)))
    s = get_settings()
    spark = get_spark(f"reconciliation_{args.ds}")
    result = compare(batch_side(spark, args.ds), stream_side(s, args.ds), args.ds, args.warn_above)
    if result.empty:
        log.warning("nothing to reconcile for %s", args.ds)
        return 0
    overwrite_partition(spark.createDataFrame(result), GOLD_RECONCILIATION, f"check_date = '{args.ds}'")
    worst = result.groupby("metric")["diff_pct"].max().to_dict()
    log.info("reconciliation %s worst diff per metric: %s", args.ds, worst)
    spark.stop()
    return 2 if max(worst.values()) > args.fail_above else 0


if __name__ == "__main__":
    sys.exit(main())
