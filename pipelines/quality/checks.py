"""Row-level expectations with quarantine + dataset-level gate.

    result = run_checks(df, CLICKSTREAM_CHECKS)
    result.valid       -> rows passing every check (continue to Silver)
    result.invalid     -> rows failing >= 1 check + `_dq_failed` array (to quarantine)
    result.summary     -> per-check failure counts (persisted to silver.dq_results)
    result.assert_gate(max_invalid_ratio=0.05)   # raise -> Airflow task fails -> Gold not rebuilt
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ecom_common.transforms import EVENT_TYPES


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    # Spark SQL boolean expression, TRUE = row is valid. Kept as a string (not a Column) so the
    # suite can be defined at import time, before any SparkSession exists.
    valid_when: str
    description: str = ""

    def expr(self) -> Column:
        return F.expr(self.valid_when)


@dataclass
class CheckResult:
    valid: DataFrame
    invalid: DataFrame
    total: int
    n_invalid: int
    summary: dict[str, int] = field(default_factory=dict)

    @property
    def invalid_ratio(self) -> float:
        return self.n_invalid / self.total if self.total else 0.0

    def assert_gate(self, max_invalid_ratio: float) -> None:
        if self.invalid_ratio > max_invalid_ratio:
            raise DataQualityError(
                f"DQ gate failed: {self.n_invalid}/{self.total} invalid rows "
                f"({self.invalid_ratio:.2%} > {max_invalid_ratio:.2%}); per check: {self.summary}"
            )

    def summary_df(self, spark, dataset: str, run_date: date) -> DataFrame:
        rows = [(dataset, run_date, name, count, self.total) for name, count in self.summary.items()]
        return spark.createDataFrame(rows, "dataset string, run_date date, check_name string, "
                                           "failed_rows long, total_rows long")


def run_checks(df: DataFrame, checks: list[Check]) -> CheckResult:
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(c.expr(), F.lit(False)), F.lit(c.name)) for c in checks
    ]))
    tagged = df.withColumn("_dq_failed", failed).cache()
    agg = tagged.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.size("_dq_failed") > 0, 1).otherwise(0)).alias("invalid"),
        *[F.sum(F.when(F.array_contains("_dq_failed", c.name), 1).otherwise(0)).alias(c.name) for c in checks],
    ).first()
    summary = {c.name: int(agg[c.name] or 0) for c in checks}
    return CheckResult(
        valid=tagged.where(F.size("_dq_failed") == 0).drop("_dq_failed"),
        invalid=tagged.where(F.size("_dq_failed") > 0),
        total=int(agg["total"] or 0),
        n_invalid=int(agg["invalid"] or 0),
        summary=summary,
    )


# ---- Suites -------------------------------------------------------------------------------
_EVENT_TYPES_SQL = ", ".join(f"'{t}'" for t in EVENT_TYPES)

CLICKSTREAM_CHECKS: list[Check] = [
    Check("event_id_not_null", "event_id IS NOT NULL"),
    Check("event_time_not_null", "event_time IS NOT NULL"),
    Check("event_time_in_range",
          "event_time BETWEEN TIMESTAMP '2019-01-01' AND current_timestamp() + INTERVAL 1 DAY",
          "REES46 starts 2019-10; future timestamps = clock skew / bad replay"),
    Check("event_type_accepted", f"event_type IN ({_EVENT_TYPES_SQL})"),
    Check("product_id_positive", "product_id > 0"),
    Check("user_id_positive", "user_id > 0"),
    Check("user_session_not_null", "user_session IS NOT NULL AND length(user_session) > 0"),
    Check("price_in_range", "price BETWEEN 0 AND 100000"),
]
