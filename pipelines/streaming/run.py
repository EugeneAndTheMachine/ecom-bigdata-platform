"""Hot-path launcher.

    spark-submit pipelines/streaming/run.py --jobs all
    spark-submit pipelines/streaming/run.py --jobs funnel,trending

In dev all queries share ONE Spark application (one driver) to save memory. In production
run one application per job (`--jobs funnel`) so each can be scaled / restarted on its own;
checkpoints are already separated per job.
"""
from __future__ import annotations

import argparse
import sys

from ecom_common.config import get_settings
from ecom_common.log import get_logger
from ecom_common.spark import get_spark
from pipelines.streaming.common import start_dlq_query
from pipelines.streaming.jobs import bot_detection, funnel, revenue, session_state, sessionization, trending

JOBS = {
    "funnel": funnel.start,
    "trending": trending.start,
    "sessionization": sessionization.start,
    "session_state": session_state.start,
    "bot": bot_detection.start,
    "revenue": revenue.start,
}
log = get_logger("hot-path")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", default="all", help=f"comma list of {sorted(JOBS)} or 'all'")
    parser.add_argument("--no-dlq", action="store_true")
    args = parser.parse_args(argv)

    selected = list(JOBS) if args.jobs.strip() == "all" else [j.strip() for j in args.jobs.split(",")]
    unknown = set(selected) - set(JOBS)
    if unknown:
        parser.error(f"unknown jobs: {unknown}")

    s = get_settings()
    spark = get_spark("hot-path-" + "-".join(selected) if len(selected) < len(JOBS) else "hot-path-all")
    queries = [JOBS[name](spark, s) for name in selected]
    if not args.no_dlq:
        queries.append(start_dlq_query(spark, s))
    for q in queries:
        log.info("started query %s (id=%s)", q.name, q.id)

    spark.streams.awaitAnyTermination()  # any failure -> exit non-zero -> container restarts
    for q in queries:
        if q.exception():
            log.error("query %s failed: %s", q.name, q.exception())
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
