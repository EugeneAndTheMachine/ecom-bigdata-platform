"""Superset configuration (mounted at /app/pythonpath/superset_config.py)."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ['SUPERSET_DB_USER']}:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@postgres:5432/{os.environ['SUPERSET_DB']}"
)

# Simple in-process cache is fine for local dev; use Redis in production.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

ROW_LIMIT = 50_000
SQLLAB_TIMEOUT = 300
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # local dev over http
