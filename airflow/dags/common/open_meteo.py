"""Shared building blocks for the Open-Meteo ingestion DAGs.

Each pipeline calls the Open-Meteo forecast API for every configured location
and lands the untouched JSON response in its own bronze table.

Bronze rows are versioned SCD Type 2 style: every run inserts a new current
version per location and closes the previous one (row_end_date, is_current).
Rows are never hard-deleted; a re-run of the same dag_run soft-deletes the
rows it replaces.
"""
import hashlib
import json
from datetime import timedelta

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_URL = "https://api.open-meteo.com/v1/forecast"
SOURCE_SYSTEM = "open-meteo"
POSTGRES_CONN_ID = "loopiq_postgres"
BRONZE_SCHEMA = "bronze"
FORECAST_DAYS = 7

LOCATIONS = [
    {"name": "London", "latitude": 51.5072, "longitude": -0.1276},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo", "latitude": 35.6762, "longitude": 139.6503},
    {"name": "Melbourne", "latitude": -37.8136, "longitude": 144.9631},
    {"name": "Sydney", "latitude": -33.8688, "longitude": 151.2093},
    {"name": "Brisbane", "latitude": -27.4698, "longitude": 153.0251},  # Queensland
]

BRONZE_DDL = """
-- Serialise DDL across DAGs that start at the same time.
SELECT pg_advisory_xact_lock(hashtext('loopiq_bronze_ddl'));

CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE FUNCTION {schema}.set_row_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.row_updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id               BIGSERIAL PRIMARY KEY,

    -- business data
    location_name    TEXT             NOT NULL,
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    request_params   JSONB            NOT NULL,
    payload          JSONB            NOT NULL,

    -- lineage
    source_system    TEXT             NOT NULL DEFAULT '{source_system}',
    source_endpoint  TEXT             NOT NULL,
    http_status_code INTEGER          NOT NULL,
    extracted_at     TIMESTAMPTZ      NOT NULL,
    record_hash      TEXT             NOT NULL,
    dag_id           TEXT             NOT NULL,
    dag_run_id       TEXT             NOT NULL,
    task_id          TEXT             NOT NULL,
    logical_date     TIMESTAMPTZ,

    -- versioning (SCD Type 2)
    row_start_date   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    row_end_date     TIMESTAMPTZ      NOT NULL DEFAULT '9999-12-31 00:00:00+00',
    is_current       BOOLEAN          NOT NULL DEFAULT TRUE,

    -- audit
    created_at       TIMESTAMPTZ      NOT NULL DEFAULT now(),
    created_by       TEXT             NOT NULL DEFAULT current_user,
    row_updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_by       TEXT             NOT NULL DEFAULT current_user,

    -- soft delete
    is_deleted       BOOLEAN          NOT NULL DEFAULT FALSE,
    deleted_at       TIMESTAMPTZ,
    deleted_by       TEXT,
    delete_reason    TEXT,

    CONSTRAINT {table}_row_dates_chk CHECK (row_end_date >= row_start_date),
    CONSTRAINT {table}_soft_delete_chk CHECK (is_deleted = (deleted_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS {table}_dag_run_id_idx ON {schema}.{table} (dag_run_id);
-- At most one live current version per location.
CREATE UNIQUE INDEX IF NOT EXISTS {table}_current_location_uidx
    ON {schema}.{table} (location_name) WHERE is_current AND NOT is_deleted;
CREATE OR REPLACE TRIGGER {table}_set_row_updated_at
    BEFORE UPDATE ON {schema}.{table}
    FOR EACH ROW EXECUTE FUNCTION {schema}.set_row_updated_at();
"""


def layer_dataset(schema: str, table: str) -> Dataset:
    """Airflow Dataset for a loopiq table; downstream DAGs schedule on these."""
    return Dataset(f"postgres://loopiq-postgres:5432/loopiq/{schema}/{table}")


def bronze_dataset(table: str) -> Dataset:
    return layer_dataset(BRONZE_SCHEMA, table)


def record_hash(payload: dict) -> str:
    """Hash of the forecast content, excluding volatile fields like generationtime_ms."""
    content = {k: v for k, v in payload.items() if k.startswith(("hourly", "daily"))}
    return hashlib.md5(json.dumps(content, sort_keys=True).encode()).hexdigest()


def make_bronze_dag(dag_id, table, api_params, schedule, description):
    """Build a DAG that extracts `api_params` for every location into bronze.`table`."""

    @dag(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
        tags=["open-meteo", "bronze"],
    )
    def pipeline():
        @task
        def create_bronze_table():
            PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(
                BRONZE_DDL.format(schema=BRONZE_SCHEMA, table=table, source_system=SOURCE_SYSTEM)
            )

        @task
        def extract(location: dict) -> dict:
            params = {
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "timezone": "auto",
                "forecast_days": FORECAST_DAYS,
                **api_params,
            }
            extracted_at = pendulum.now("UTC").isoformat()
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            return {
                "location": location,
                "request_params": params,
                "payload": response.json(),
                "http_status_code": response.status_code,
                "extracted_at": extracted_at,
            }

        @task(outlets=[bronze_dataset(table)])
        def load_bronze(records: list[dict]):
            context = get_current_context()
            run_id = context["run_id"]
            actor = f"airflow:{dag_id}"
            target = f"{BRONZE_SCHEMA}.{table}"
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            # One transaction, so now() is identical for the closed and the new version.
            with hook.get_conn() as conn, conn.cursor() as cur:
                for r in records:
                    keys = {"run_id": run_id, "location": r["location"]["name"], "actor": actor}
                    # A re-run replaces this run's earlier rows: soft-delete them.
                    cur.execute(
                        f"""
                        UPDATE {target}
                        SET is_deleted = TRUE, deleted_at = now(), deleted_by = %(actor)s,
                            delete_reason = 'superseded by re-run of the same dag_run',
                            is_current = FALSE, row_end_date = now(), updated_by = %(actor)s
                        WHERE dag_run_id = %(run_id)s AND location_name = %(location)s AND NOT is_deleted
                        """,
                        keys,
                    )
                    # Close the previous current version for this location.
                    cur.execute(
                        f"""
                        UPDATE {target}
                        SET is_current = FALSE, row_end_date = now(), updated_by = %(actor)s
                        WHERE location_name = %(location)s AND is_current AND NOT is_deleted
                        """,
                        keys,
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {target} (
                            location_name, latitude, longitude, request_params, payload,
                            source_endpoint, http_status_code, extracted_at, record_hash,
                            dag_id, dag_run_id, task_id, logical_date, created_by, updated_by
                        ) VALUES (
                            %s, %s, %s, %s::jsonb, %s::jsonb,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            r["location"]["name"],
                            r["location"]["latitude"],
                            r["location"]["longitude"],
                            json.dumps(r["request_params"]),
                            json.dumps(r["payload"]),
                            API_URL,
                            r["http_status_code"],
                            r["extracted_at"],
                            record_hash(r["payload"]),
                            dag_id,
                            run_id,
                            context["ti"].task_id,
                            context.get("logical_date"),
                            actor,
                            actor,
                        ),
                    )
            print(f"Loaded {len(records)} rows into {target} for run {run_id}")

        create_bronze_table() >> load_bronze(extract.expand(location=LOCATIONS))

    return pipeline()
