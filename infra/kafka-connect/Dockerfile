ARG CP_VERSION=7.9.1
FROM confluentinc/cp-kafka-connect:${CP_VERSION}

ARG S3_SINK_CONNECTOR_VERSION=latest
# Pin a concrete version (e.g. 10.5.23) for reproducible builds once you validated it.
RUN confluent-hub install --no-prompt "confluentinc/kafka-connect-s3:${S3_SINK_CONNECTOR_VERSION}"
