#!/bin/bash
# Runs once on first container start (empty data dir). One role + one DB per consumer.
set -euo pipefail

create_db() {
  local db="$1" user="$2" pass="$3"
  echo ">> creating database ${db} owned by ${user}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    DO \$\$ BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${user}') THEN
        CREATE ROLE ${user} LOGIN PASSWORD '${pass}';
      END IF;
    END \$\$;
    SELECT 'CREATE DATABASE ${db} OWNER ${user}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
SQL
}

create_db "${METASTORE_DB}" "${METASTORE_DB_USER}" "${METASTORE_DB_PASSWORD}"
create_db "${AIRFLOW_DB}"   "${AIRFLOW_DB_USER}"   "${AIRFLOW_DB_PASSWORD}"
create_db "${MLFLOW_DB}"    "${MLFLOW_DB_USER}"    "${MLFLOW_DB_PASSWORD}"
create_db "${SUPERSET_DB}"  "${SUPERSET_DB_USER}"  "${SUPERSET_DB_PASSWORD}"
