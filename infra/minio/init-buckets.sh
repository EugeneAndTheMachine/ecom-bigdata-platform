#!/bin/sh
# Create buckets (idempotent) with the AWS CLI against MinIO.
set -eu
EP="${S3_ENDPOINT:-http://minio:9000}"

for b in "${LAKEHOUSE_BUCKET:-lakehouse}" "${CHECKPOINT_BUCKET:-checkpoints}" "${MLFLOW_BUCKET:-mlflow}"; do
  if aws --endpoint-url "$EP" s3api head-bucket --bucket "$b" 2>/dev/null; then
    echo "bucket $b exists"
  else
    aws --endpoint-url "$EP" s3api create-bucket --bucket "$b"
    echo "bucket $b created"
  fi
done

# Prefix markers so the layout is visible in the console from day one
for p in landing/ bronze/ silver/ gold/ ml/ quarantine/; do
  aws --endpoint-url "$EP" s3api put-object --bucket "${LAKEHOUSE_BUCKET:-lakehouse}" --key "$p" >/dev/null
done
echo "MinIO ready."
