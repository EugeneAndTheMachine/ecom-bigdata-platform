"""Batch scoring: champion ALS -> top-N candidates per active user -> Delta + Redis recs:{user}.

The Recommend API re-ranks these candidates online with the live session context.
"""
from __future__ import annotations

import sys

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from ecom_common.tables import ML_USER_RECS, SILVER_EVENTS, SILVER_PRODUCTS_SCD2
from pipelines.ml.als_recommender import model_root
from pipelines.streaming.sinks.redis_sink import client_from

log = get_logger("batch_scoring")
ACTIVE_DAYS = 30
RECS_TTL_S = 8 * 24 * 3600


def main() -> int:
    s = get_settings()
    spark = get_spark("ml_batch_scoring")
    model = ALSModel.load(f"{model_root()}/champion")
    ev = spark.read.format("delta").load(SILVER_EVENTS.path).where("NOT is_bot")
    # "active" per timeline (replay = now, bulk = 2019-10..2020-04): a single max over both
    # would only ever keep replay users
    last_day = ev.groupBy("_source").agg(F.max("event_date").alias("last_day"))
    active = (ev.join(F.broadcast(last_day), "_source")
              .where(F.col("event_date") > F.date_sub(F.col("last_day"), ACTIVE_DAYS))
              .select(F.col("user_id").cast("int").alias("user_id")).distinct())

    recs = (model.recommendForUserSubset(active, s.recs_top_n)
            .select("user_id", F.posexplode("recommendations").alias("pos", "rec"))
            .select("user_id", F.col("rec.product_id").alias("product_id"),
                    F.col("rec.rating").alias("score"), (F.col("pos") + 1).alias("rank"))
            .withColumn("scored_at", F.current_timestamp())
            .cache())
    recs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ML_USER_RECS.path)

    redis_conf = (s.serving.redis_host, s.serving.redis_port, s.serving.redis_password)

    def push(rows) -> None:  # executors
        r = client_from(*redis_conf)
        pipe = r.pipeline(transaction=False)
        for i, row in enumerate(rows, 1):
            pipe.set(f"recs:{row.user_id}", row.payload, ex=RECS_TTL_S)
            if i % 1000 == 0:
                pipe.execute()
        pipe.execute()

    (recs.groupBy("user_id")
         .agg(F.sort_array(F.collect_list(F.struct("rank", "product_id", "score"))).alias("items"))
         # payload contract with services/recommend-api: [{"p": product_id, "s": score}, ...] (rank order)
         .select("user_id", F.to_json(F.transform(
             "items", lambda x: F.struct(x["product_id"].alias("p"), x["score"].alias("s")))).alias("payload"))
         .foreachPartition(push))

    # product -> category_l1 lookup used by the API's category boost (candidates only)
    dim = (spark.read.format("delta").load(SILVER_PRODUCTS_SCD2.path).where("is_current")
           .select(F.col("product_id").cast("int").alias("product_id"), "category_l1"))
    cats = recs.select("product_id").distinct().join(dim, "product_id").collect()
    r = client_from(*redis_conf)
    for i in range(0, len(cats), 5000):
        chunk = {str(c.product_id): c.category_l1 for c in cats[i:i + 5000]}
        if chunk:
            r.hset("product:category", mapping=chunk)
    log.info("scored %d users, %d candidate products", recs.select("user_id").distinct().count(), len(cats))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
