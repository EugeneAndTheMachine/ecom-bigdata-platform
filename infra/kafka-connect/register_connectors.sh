#!/bin/sh
# Registers (create-or-update via PUT) the S3 sink connectors that land Kafka topics
# in MinIO = the immutable "landing" zone the Bronze job reads from.
#
#   s3://lakehouse/landing/topics/<topic>/dt=YYYY-MM-dd/hr=HH/<topic>+<partition>+<startOffset>.<ext>
#
# Partition by Kafka record timestamp (= ingest time), NOT event time:
# late events are therefore never "lost" in an already-closed partition.
set -eu

CONNECT="${CONNECT_URL:-http://kafka-connect:8083}"
BUCKET="${LAKEHOUSE_BUCKET:-lakehouse}"
REGION="${S3_REGION:-us-east-1}"
ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"

until curl -fsS "${CONNECT}/connectors" >/dev/null; do echo "waiting for kafka-connect..."; sleep 5; done

put() {
  name="$1"; body="$2"
  echo ">> PUT ${name}"
  curl -fsS -X PUT -H "Content-Type: application/json" --data "${body}" "${CONNECT}/connectors/${name}/config"
  echo
}

COMMON_S3=$(cat <<EOF
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "storage.class": "io.confluent.connect.s3.storage.S3Storage",
  "s3.bucket.name": "${BUCKET}",
  "s3.region": "${REGION}",
  "store.url": "${ENDPOINT}",
  "s3.path.style.access.enabled": "true",
  "topics.dir": "landing/topics",
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "path.format": "'dt'=YYYY-MM-dd/'hr'=HH",
  "partition.duration.ms": "3600000",
  "locale": "en-US",
  "timezone": "UTC",
  "timestamp.extractor": "Record",
  "rotate.schedule.interval.ms": "60000",
  "behavior.on.null.values": "ignore",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "transforms": "meta",
  "transforms.meta.type": "org.apache.kafka.connect.transforms.InsertField\$Value",
  "transforms.meta.offset.field": "_kafka_offset",
  "transforms.meta.partition.field": "_kafka_partition",
  "transforms.meta.timestamp.field": "_kafka_ts"
EOF
)

# 1) Clickstream (Avro + Schema Registry) -> Parquet
put "s3-sink-clickstream" "{
  ${COMMON_S3},
  \"topics\": \"${TOPIC_CLICKSTREAM:-ecom.clickstream.v1}\",
  \"tasks.max\": \"2\",
  \"format.class\": \"io.confluent.connect.s3.format.parquet.ParquetFormat\",
  \"parquet.codec\": \"snappy\",
  \"flush.size\": \"50000\",
  \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
  \"value.converter\": \"io.confluent.connect.avro.AvroConverter\",
  \"value.converter.schema.registry.url\": \"${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}\"
}"

# 2) Hot-path outputs (schemaless JSON) -> JSON lines
put "s3-sink-hotpath-outputs" "{
  ${COMMON_S3},
  \"topics\": \"${TOPIC_ALERT_CART:-alerts.cart_abandon},${TOPIC_ALERT_BOT:-alerts.bot},${TOPIC_SESSIONS:-agg.sessions},${TOPIC_RECO_IMPRESSIONS:-reco.impressions},${TOPIC_CLICKSTREAM_DLQ:-ecom.clickstream.dlq}\",
  \"tasks.max\": \"1\",
  \"format.class\": \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\": \"5000\",
  \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
  \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
  \"value.converter.schemas.enable\": \"false\"
}"

curl -fsS "${CONNECT}/connectors?expand=status"; echo
echo "Connectors registered."
