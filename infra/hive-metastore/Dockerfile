# Hive Metastore 4.0.0 (NOT 4.0.1+: 4.0.1 removed get_table / get_table_objects_by_name,
# breaking older thrift clients). Only Trino talks to HMS in this design (see ADR-0003).
ARG HIVE_IMAGE=apache/hive:4.0.0
FROM ${HIVE_IMAGE}

ARG HIVE_HADOOP_VERSION=3.3.6
ARG HIVE_AWS_SDK_VERSION=1.12.367
ARG POSTGRES_JDBC_VERSION=42.7.4

USER root
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/
# S3A so HMS can validate/create s3a:// schema locations on MinIO
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HIVE_HADOOP_VERSION}/hadoop-aws-${HIVE_HADOOP_VERSION}.jar /opt/hive/lib/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${HIVE_AWS_SDK_VERSION}/aws-java-sdk-bundle-${HIVE_AWS_SDK_VERSION}.jar /opt/hive/lib/
USER hive
