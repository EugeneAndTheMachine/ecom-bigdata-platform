"""Data-aware scheduling: DAGs downstream of the medallion run trigger on these Assets."""
from airflow.sdk import Asset

SILVER_EVENTS = Asset("s3a://lakehouse/silver/events")
GOLD_MARTS = Asset("trino://trino:8080/delta/gold")
ML_RECS = Asset("s3a://lakehouse/ml/user_recs")
