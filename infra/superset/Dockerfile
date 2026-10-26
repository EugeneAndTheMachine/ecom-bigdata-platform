ARG SUPERSET_VERSION=4.1.2
FROM apache/superset:${SUPERSET_VERSION}

USER root
# Drivers: Trino (lakehouse Gold), ClickHouse (speed layer), Postgres (metadata DB)
# Superset >= 5 installs into /app/.venv via uv; 4.x uses the system pip.
RUN if [ -x /app/.venv/bin/python ] && command -v uv >/dev/null 2>&1; then \
      uv pip install --python /app/.venv/bin/python --no-cache trino clickhouse-connect psycopg2-binary; \
    else \
      pip install --no-cache-dir trino clickhouse-connect psycopg2-binary; \
    fi
USER superset
