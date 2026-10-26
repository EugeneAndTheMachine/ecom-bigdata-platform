# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# One Spark image for master, worker, streaming driver, batch jobs and Jupyter.
# Built from python:<ver>-slim + PyPI pyspark so that driver (Airflow / Jupyter)
# and executors share EXACTLY the same Python minor version (PySpark requirement).
# -----------------------------------------------------------------------------
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

ARG SPARK_VERSION=3.5.5
ARG DELTA_VERSION=3.3.2
ARG SCALA_BINARY=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG MLFLOW_VERSION=2.22.0

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 # multi-arch: JAVA_HOME differs on arm64
 && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY infra/spark/requirements.txt /tmp/requirements.txt
RUN pip install "pyspark==${SPARK_VERSION}" "delta-spark==${DELTA_VERSION}" "mlflow==${MLFLOW_VERSION}" -r /tmp/requirements.txt

# SPARK_HOME = the pyspark package dir (contains jars/ and bin/)
RUN SPARK_HOME="$(python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')" \
 && ln -s "${SPARK_HOME}" /opt/spark
ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:${PATH} \
    PYTHONPATH=/opt/app/libs:/opt/app \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python

# Extra JARs baked into $SPARK_HOME/jars (no --packages at runtime = no Ivy downloads)
COPY infra/spark/download_jars.sh /tmp/download_jars.sh
RUN SPARK_VERSION=${SPARK_VERSION} DELTA_VERSION=${DELTA_VERSION} SCALA_BINARY=${SCALA_BINARY} \
    HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION} AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION} \
    bash /tmp/download_jars.sh /opt/spark/jars

COPY infra/spark/conf/ /opt/spark/conf/

WORKDIR /opt/app
EXPOSE 4040 7077 8080 8081
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]

# -----------------------------------------------------------------------------
FROM runtime AS jupyter
RUN pip install jupyterlab==4.2.5 matplotlib==3.9.2 seaborn==0.13.2 trino==0.330.0
EXPOSE 8888
