#!/usr/bin/env bash
# Airflow container entrypoint: migrate the metadata DB, make sure the admin
# user and the loopiq_postgres connection exist, then run Airflow standalone.
set -euo pipefail

airflow db migrate

# No-op (prints "already exist") when the user is already there.
airflow users create \
  --username "$AIRFLOW_ADMIN_USERNAME" --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname Admin --lastname User --role Admin --email admin@example.com

# Stored (encrypted with the Fernet key) in Airflow's Connections, used by every DAG.
if ! airflow connections get loopiq_postgres >/dev/null 2>&1; then
  airflow connections add loopiq_postgres \
    --conn-type postgres \
    --conn-description "Local Postgres - loopiq medallion database (bronze/silver/gold)" \
    --conn-host loopiq-postgres --conn-port 5432 --conn-schema "$POSTGRES_DB" \
    --conn-login "$POSTGRES_USER" --conn-password "$POSTGRES_PASSWORD"
fi

exec airflow standalone
