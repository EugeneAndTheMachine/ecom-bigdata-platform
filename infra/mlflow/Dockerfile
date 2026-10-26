ARG MLFLOW_VERSION=2.22.0
FROM ghcr.io/mlflow/mlflow:v${MLFLOW_VERSION}
# Postgres backend store + S3 (MinIO) artifact store
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 boto3==1.35.36
