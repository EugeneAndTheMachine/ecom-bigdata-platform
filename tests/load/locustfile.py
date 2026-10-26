"""Load test for the Recommend API (target: p95 < 100 ms at 200 RPS on a laptop).

    pip install locust==2.32.2
    locust -f tests/load/locustfile.py --host http://localhost:8000 -u 200 -r 20 -t 2m --headless
User ids are sampled from a file exported from ml.user_recs (warm) mixed with random ids (cold start):
    docker compose exec trino trino --output-format TSV \
      --execute "SELECT user_id FROM delta.ml.user_recs LIMIT 5000" > data/sample/warm_users.tsv
"""
from __future__ import annotations

import random
from pathlib import Path

from locust import HttpUser, between, task

WARM = [int(x) for x in Path("data/sample/warm_users.tsv").read_text().split()] \
    if Path("data/sample/warm_users.tsv").exists() else []


class Shopper(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(4)
    def warm_user(self):
        uid = random.choice(WARM) if WARM else random.randint(1, 600_000_000)
        self.client.get(f"/recommend/{uid}?k=10", name="/recommend/[warm]")

    @task(1)
    def cold_user(self):
        self.client.get(f"/recommend/{random.randint(1, 10_000)}?k=10", name="/recommend/[cold]")
