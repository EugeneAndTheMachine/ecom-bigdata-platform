#!/usr/bin/env bash
# Idempotent topic creation. Runs once in the `kafka-init` container.
set -euo pipefail

BS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"
BIN=/opt/kafka/bin
DAY_MS=86400000

create() {
  local topic="$1" partitions="$2" retention_ms="$3"; shift 3
  echo ">> ${topic} (p=${partitions}, rf=${RF}, retention=${retention_ms}ms)"
  "${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --create --if-not-exists \
    --topic "${topic}" --partitions "${partitions}" --replication-factor "${RF}" \
    --config "retention.ms=${retention_ms}" "$@"
}

# F1 – source of truth (key = user_session)
create "${TOPIC_CLICKSTREAM:-ecom.clickstream.v1}"     "${CLICKSTREAM_PARTITIONS:-12}" $((7 * DAY_MS)) --config compression.type=lz4
create "${TOPIC_CLICKSTREAM_DLQ:-ecom.clickstream.dlq}" 3 $((14 * DAY_MS))
# F4 – outputs of the hot path
create "${TOPIC_SESSIONS:-agg.sessions}"               6 $((7 * DAY_MS))
create "${TOPIC_ALERT_CART:-alerts.cart_abandon}"      3 $((7 * DAY_MS))
create "${TOPIC_ALERT_BOT:-alerts.bot}"                3 $((7 * DAY_MS))
# F6 – feedback loop from the recommend API
create "${TOPIC_RECO_IMPRESSIONS:-reco.impressions}"   3 $((7 * DAY_MS))

"${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --list
echo "Topics ready."
