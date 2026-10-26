# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# Airflow 3 + Spark client (spark-submit, same pyspark/Python as the cluster)
# + dbt-trino in an ISOLATED virtualenv (avoids dependency clashes with Airflow).
# -----------------------------------------------------------------------------
ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=3.5.5
ARG DELTA_VERSION=3.3.2
ARG SCALA_BINARY=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG DBT_TRINO_VERSION=1.9.0
ARG MLFLOW_VERSION=2.22.0

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# dbt lives in its own venv: /opt/dbt-venv/bin/dbt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-trino==${DBT_TRINO_VERSION}" "dbt-core>=1.10,<2" \
 && chown -R airflow:0 /opt/dbt-venv

USER airflow
COPY infra/airflow/requirements.txt /tmp/requirements.txt
# Use Airflow constraints so providers never downgrade/upgrade core deps by accident
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "pyspark==${SPARK_VERSION}" "delta-spark==${DELTA_VERSION}" "mlflow==${MLFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
   || pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      "pyspark==${SPARK_VERSION}" "delta-spark==${DELTA_VERSION}" "mlflow==${MLFLOW_VERSION}" -r /tmp/requirements.txt

USER root
COPY infra/spark/download_jars.sh /tmp/download_jars.sh
COPY infra/spark/conf/ /tmp/spark-conf/
RUN SPARK_HOME="$(python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')" \
 && SPARK_VERSION=${SPARK_VERSION} DELTA_VERSION=${DELTA_VERSION} SCALA_BINARY=${SCALA_BINARY} \
    HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION} \
    bash /tmp/download_jars.sh "${SPARK_HOME}/jars" \
 && mkdir -p "${SPARK_HOME}/conf" && cp /tmp/spark-conf/* "${SPARK_HOME}/conf/" \
 && ln -sfn "${SPARK_HOME}" /opt/spark \
 && mkdir -p /opt/app && chown -R airflow:0 /opt/app
ENV SPARK_HOME=/opt/spark
USER airflow
