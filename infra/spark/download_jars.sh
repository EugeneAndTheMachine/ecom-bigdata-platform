#!/usr/bin/env bash
# Download the JVM dependencies that PyPI pyspark does not ship with.
# Usage: download_jars.sh <target_dir>   (versions come from env vars)
set -euo pipefail

TARGET="${1:-/opt/spark/jars}"
MVN="https://repo1.maven.org/maven2"
: "${SPARK_VERSION:?}" "${DELTA_VERSION:?}" "${SCALA_BINARY:?}" "${HADOOP_AWS_VERSION:?}" "${AWS_SDK_BUNDLE_VERSION:?}"

JARS=(
  # Delta Lake
  "io/delta/delta-spark_${SCALA_BINARY}/${DELTA_VERSION}/delta-spark_${SCALA_BINARY}-${DELTA_VERSION}.jar"
  "io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"
  # S3A (MinIO). hadoop-aws must match the Hadoop version bundled with pyspark (3.3.4 for Spark 3.5)
  "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
  "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
  # Kafka source/sink for Structured Streaming
  "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  # Avro (from_avro / to_avro)
  "org/apache/spark/spark-avro_${SCALA_BINARY}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY}-${SPARK_VERSION}.jar"
)

mkdir -p "${TARGET}"
for path in "${JARS[@]}"; do
  file="$(basename "${path}")"
  echo ">> ${file}"
  curl -fsSL --retry 5 --retry-delay 3 -o "${TARGET}/${file}" "${MVN}/${path}"
done
echo "Downloaded ${#JARS[@]} jars into ${TARGET}"
