"""Pure column transformations shared by the speed layer and the batch layer.

Keeping them in ONE place is what guarantees the hot path and the cold path compute the
same thing (and what makes the nightly reconciliation meaningful).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")
# Implicit-feedback weights shared by trending score and ALS ratings
EVENT_WEIGHTS = {"view": 1.0, "cart": 3.0, "remove_from_cart": 0.0, "purchase": 5.0}
UNKNOWN = "unknown"


def category_levels(category_code: str | Column = "category_code") -> list[Column]:
    """'electronics.audio.headphone' -> l1=electronics, l2=audio, l3=headphone (unknown if null)."""
    code = F.col(category_code) if isinstance(category_code, str) else category_code
    parts = F.split(F.coalesce(code, F.lit(UNKNOWN)), r"\.")
    return [
        F.coalesce(parts.getItem(0), F.lit(UNKNOWN)).alias("category_l1"),
        F.coalesce(parts.getItem(1), F.lit(UNKNOWN)).alias("category_l2"),
        F.coalesce(parts.getItem(2), F.lit(UNKNOWN)).alias("category_l3"),
    ]


def with_category_levels(df: DataFrame) -> DataFrame:
    return df.select("*", *category_levels())


def event_weight(event_type: str | Column = "event_type") -> Column:
    et = F.col(event_type) if isinstance(event_type, str) else event_type
    expr = F.lit(0.0)
    for name, weight in EVENT_WEIGHTS.items():
        expr = F.when(et == name, F.lit(weight)).otherwise(expr)
    return expr


def count_if(event_type: str, alias: str) -> Column:
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).cast("long").alias(alias)


def funnel_aggs() -> list[Column]:
    return [
        count_if("view", "views"),
        count_if("cart", "carts"),
        count_if("remove_from_cart", "removes"),
        count_if("purchase", "purchases"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0.0)).alias("revenue"),
    ]


def normalize_brand(col: str = "brand") -> Column:
    return F.coalesce(F.lower(F.trim(F.col(col))), F.lit(UNKNOWN))


def natural_key() -> list[str]:
    """Dedup key in Silver: identical events from replay duplicates or the source file itself."""
    return ["user_session", "event_time", "product_id", "event_type"]


def event_id_col(event_time: str = "event_time") -> Column:
    """sha256 of natural_key() — the replay producer emits the same value
    (producer.transform.event_id_for), so the stream's dedup on event_id and Silver's dedup on
    natural_key() remove the same rows. Needs a UTC Spark session.
    """
    return F.sha2(F.concat_ws("|", F.col("user_session"), F.col(event_time).cast("string"),
                              F.col("product_id").cast("string"), F.col("event_type")), 256)
