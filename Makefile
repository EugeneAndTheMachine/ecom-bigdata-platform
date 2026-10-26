# ---------------------------------------------------------------------------
# ecom-bigdata-platform — developer entrypoints.  `make help` lists targets.
# ---------------------------------------------------------------------------
SHELL := /bin/bash
COMPOSE := docker compose
ALL_PROFILES := --profile stream --profile replay --profile batch --profile bi --profile ml --profile obs
DS ?= $(shell date -u -d 'yesterday' +%F 2>/dev/null || date -u -v-1d +%F)
INPUT ?= /data/raw/2019-Oct.csv.gz
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master

.DEFAULT_GOAL := help
.PHONY: help init env build up-core up-stream replay up-batch up-bi up-ml up-obs up-all ps logs down clean \
        sample download backfill bronze silver dbt register medallion recon ml smoke test lint fmt

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

# ----- setup ----------------------------------------------------------------
env: ## Create .env from .env.example (generates a Fernet key) if missing
	@test -f .env || { cp .env.example .env; \
	  key=$$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())'); \
	  sed -i.bak "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$key|" .env && rm -f .env.bak; \
	  echo ".env created"; }

init: env ## First-time setup: .env, data dirs, build images
	@mkdir -p data/raw data/sample
	$(COMPOSE) $(ALL_PROFILES) build

build: ## Rebuild all custom images
	$(COMPOSE) $(ALL_PROFILES) build

download: ## Download REES46 from Kaggle into data/raw
	./scripts/download_rees46.sh

sample: ## Make a 1% / 3-day session-complete sample in data/sample
	python3 scripts/make_sample.py --input data/raw/2019-Oct.csv.gz --output data/sample/rees46_sample.csv.gz

# ----- run ------------------------------------------------------------------
up-core: ## Kafka, Schema Registry, MinIO, Postgres, Redis, ClickHouse
	$(COMPOSE) up -d

up-stream: ## + Kafka Connect, Spark cluster, streaming jobs (hot path)
	$(COMPOSE) --profile stream up -d

replay: ## Start the REES46 replay producer (REPLAY_SPEED in .env)
	$(COMPOSE) --profile stream --profile replay up -d clickstream-producer

up-batch: ## + Hive Metastore, Trino, Airflow (cold path)
	$(COMPOSE) --profile stream --profile batch up -d

up-bi: ## + Grafana, Superset, Jupyter
	$(COMPOSE) --profile batch --profile bi up -d

up-ml: ## + MLflow, Recommend API
	$(COMPOSE) --profile ml up -d

up-obs: ## + Prometheus, kafka-exporter, kafka-ui
	$(COMPOSE) --profile obs up -d

up-all: ## Everything (needs ~16 GB RAM for Docker)
	$(COMPOSE) $(ALL_PROFILES) up -d

ps: ## Service status
	$(COMPOSE) $(ALL_PROFILES) ps

logs: ## Tail logs: make logs s=spark-streaming
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=200 $(s)

down: ## Stop everything (keep volumes)
	$(COMPOSE) $(ALL_PROFILES) down

clean: ## Stop and DELETE all volumes (lakehouse, kafka, dbs)
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

# ----- batch (manual runs; Airflow does the same on schedule) ---------------
backfill: ## Bulk-load a CSV into Bronze: make backfill INPUT=/data/raw/2019-Nov.csv.gz
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/bulk_backfill.py --input $(INPUT)

bronze: ## Landing -> Bronze for DS (default yesterday): make bronze DS=2026-10-25
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/bronze_load.py --ds $(DS)

silver: ## Bronze -> Silver (events, sessions, products SCD2) for DS
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/silver_events.py --ds $(DS)
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/silver_sessions.py --ds $(DS)
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/silver_products_scd2.py --ds $(DS)

register: ## Register Spark-written Delta tables in Trino
	$(COMPOSE) exec -T airflow-scheduler bash -c "cd /opt/app && python -m pipelines.batch.register_tables"

dbt: ## dbt build (gold + tests) through Trino
	$(COMPOSE) exec -T airflow-scheduler /opt/dbt-venv/bin/dbt build \
	  --project-dir /opt/app/pipelines/dbt --profiles-dir /opt/app/pipelines/dbt --target-path /tmp/dbt/target

recon: ## Batch vs speed layer reconciliation for DS
	$(SPARK_SUBMIT) /opt/app/pipelines/batch/reconciliation.py --ds $(DS)

medallion: bronze register silver register dbt recon ## Whole daily chain for DS without Airflow

ml: ## Train segmentation, intent, ALS and score recommendations
	$(SPARK_SUBMIT) /opt/app/pipelines/ml/segmentation.py
	$(SPARK_SUBMIT) /opt/app/pipelines/ml/purchase_intent.py
	$(SPARK_SUBMIT) /opt/app/pipelines/ml/als_recommender.py
	$(SPARK_SUBMIT) /opt/app/pipelines/ml/batch_scoring.py

# ----- quality --------------------------------------------------------------
smoke: ## End-to-end health/data-flow checks
	./scripts/smoke_test.sh

test: ## Unit tests (needs requirements-dev.txt + Java 17)
	python3 -m pytest

lint: ## Ruff
	ruff check .

fmt: ## Ruff autofix + format
	ruff check --fix . && ruff format .
