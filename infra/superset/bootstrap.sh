#!/bin/bash
# Idempotent Superset bootstrap: migrate metadata DB, create admin, register data sources, run.
set -euo pipefail

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" --firstname Admin --lastname User \
  --email admin@example.com --password "${SUPERSET_ADMIN_PASSWORD}" || true
superset init

# Data sources (set-database-uri is create-or-update)
superset set-database-uri -d "Lakehouse (Trino)" \
  -u "trino://superset@${TRINO_HOST:-trino}:${TRINO_INTERNAL_PORT:-8080}/delta" || true
superset set-database-uri -d "Realtime (ClickHouse)" \
  -u "clickhousedb://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST:-clickhouse}:8123/${CLICKHOUSE_DB:-realtime}" || true

exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 --limit-request-line 0 \
  --limit-request-field_size 0 "superset.app:create_app()"
