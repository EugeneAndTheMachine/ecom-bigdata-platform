#!/usr/bin/env bash
# End-to-end smoke test: "is every hop of the pipeline alive and is data flowing?"
# Usage: ./scripts/smoke_test.sh [core|stream|batch|ml|all]   (default: all running services)
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PASS=0; FAIL=0
ok()   { echo "  [OK]   $1"; PASS=$((PASS+1)); }
ko()   { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
check(){ local name=$1; shift; if "$@" >/dev/null 2>&1; then ok "$name"; else ko "$name"; fi; }
running(){ docker compose ps --status running --services 2>/dev/null | grep -qx "$1"; }
ch(){ curl -sf -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" "http://localhost:${CLICKHOUSE_HTTP_PORT}/" --data-binary "$1"; }

echo "== core"
check "minio live"            curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live"
check "kafka topics"          docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "${TOPIC_CLICKSTREAM}"
check "schema registry"       curl -sf "http://localhost:${SCHEMA_REGISTRY_PORT}/subjects"
check "redis ping"            docker compose exec -T redis redis-cli -a "${REDIS_PASSWORD}" --no-auth-warning ping
check "clickhouse ping"       curl -sf "http://localhost:${CLICKHOUSE_HTTP_PORT}/ping"
check "clickhouse schema"     ch "EXISTS TABLE ${CLICKHOUSE_DB}.rt_funnel_1m"

if running kafka-connect; then
  echo "== stream"
  check "connect s3 sink RUNNING" bash -c "curl -sf http://localhost:${KAFKA_CONNECT_PORT}/connectors/s3-sink-clickstream/status | grep -q '\"state\":\"RUNNING\"'"
fi
if running clickstream-producer; then
  check "producer metrics"     curl -sf "http://localhost:${PRODUCER_METRICS_PORT}/metrics"
  check "avro subject registered" bash -c "curl -sf http://localhost:${SCHEMA_REGISTRY_PORT}/subjects | grep -q ${TOPIC_CLICKSTREAM}-value"
fi
if running spark-streaming; then
  n=$(ch "SELECT count() FROM ${CLICKHOUSE_DB}.rt_funnel_1m WHERE window_start > now() - INTERVAL 10 MINUTE" || echo 0)
  if [ "${n:-0}" -gt 0 ]; then ok "rt_funnel_1m has fresh rows ($n)"; else ko "rt_funnel_1m has no rows in last 10 min"; fi
  k=$(docker compose exec -T redis redis-cli -a "${REDIS_PASSWORD}" --no-auth-warning --scan --pattern 'trending:*' --count 1000 | head -1)
  if [ -n "$k" ]; then ok "redis trending keys"; else ko "redis trending keys"; fi
  check "landing files in MinIO" docker compose run --rm --no-deps -T --entrypoint aws minio-init \
        --endpoint-url http://minio:9000 s3 ls "s3://${LAKEHOUSE_BUCKET}/landing/topics/${TOPIC_CLICKSTREAM}/"
fi

if running trino; then
  echo "== batch"
  check "trino info"           curl -sf "http://localhost:${TRINO_PORT}/v1/info"
  check "trino delta catalog"  docker compose exec -T trino trino --execute "SHOW SCHEMAS FROM delta"
fi
if running airflow-apiserver; then
  check "airflow health"       curl -sf "http://localhost:${AIRFLOW_PORT}/api/v2/monitor/health"
fi

if running recommend-api; then
  echo "== ml"
  check "recommend-api health" curl -sf "http://localhost:${RECOMMEND_API_PORT}/health"
  check "recommend cold start" curl -sf "http://localhost:${RECOMMEND_API_PORT}/recommend/1?k=5"
  check "mlflow"               curl -sf "http://localhost:${MLFLOW_PORT}/health"
fi

echo "== result: ${PASS} passed, ${FAIL} failed"
[ "$FAIL" -eq 0 ]
