"""Bronze -> silver transformation for the Open-Meteo data.

Each silver table flattens one section (hourly or daily) of a bronze table's
JSON payload into one typed row per location per hour/day.

Silver rows are versioned SCD Type 2 style at that grain: when a newer
forecast changes a value, the current version is closed and a new one is
inserted; unchanged values are left alone. Bronze rows are processed in id
order, one transaction each, and recorded in silver.etl_load_log, which makes
the load incremental and safe to retry.
"""
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.open_meteo import POSTGRES_CONN_ID

SILVER_SCHEMA = "silver"
LOAD_LOG = f"{SILVER_SCHEMA}.etl_load_log"

# silver table -> bronze source, payload section, and measures as
# (silver column, API field, SQL type, expected API unit).
SILVER_TABLES = {
    "temperature_hourly": {
        "bronze_table": "temperature_raw",
        "section": "hourly",
        "measures": [
            ("temperature_c", "temperature_2m", "NUMERIC(6,2)", "°C"),
            ("apparent_temperature_c", "apparent_temperature", "NUMERIC(6,2)", "°C"),
            ("dew_point_c", "dew_point_2m", "NUMERIC(6,2)", "°C"),
        ],
    },
    "temperature_daily": {
        "bronze_table": "temperature_raw",
        "section": "daily",
        "measures": [
            ("temperature_max_c", "temperature_2m_max", "NUMERIC(6,2)", "°C"),
            ("temperature_min_c", "temperature_2m_min", "NUMERIC(6,2)", "°C"),
        ],
    },
    "rain_hourly": {
        "bronze_table": "rain_raw",
        "section": "hourly",
        "measures": [
            ("precipitation_mm", "precipitation", "NUMERIC(8,2)", "mm"),
            ("rain_mm", "rain", "NUMERIC(8,2)", "mm"),
            ("showers_mm", "showers", "NUMERIC(8,2)", "mm"),
            ("precipitation_probability_pct", "precipitation_probability", "SMALLINT", "%"),
        ],
    },
    "rain_daily": {
        "bronze_table": "rain_raw",
        "section": "daily",
        "measures": [
            ("precipitation_sum_mm", "precipitation_sum", "NUMERIC(8,2)", "mm"),
            ("rain_sum_mm", "rain_sum", "NUMERIC(8,2)", "mm"),
            ("showers_sum_mm", "showers_sum", "NUMERIC(8,2)", "mm"),
            ("precipitation_hours", "precipitation_hours", "NUMERIC(5,2)", "h"),
            ("precipitation_probability_max_pct", "precipitation_probability_max", "SMALLINT", "%"),
        ],
    },
    "wind_hourly": {
        "bronze_table": "wind_raw",
        "section": "hourly",
        "measures": [
            ("wind_speed_kmh", "wind_speed_10m", "NUMERIC(6,2)", "km/h"),
            ("wind_direction_deg", "wind_direction_10m", "SMALLINT", "°"),
            ("wind_gusts_kmh", "wind_gusts_10m", "NUMERIC(6,2)", "km/h"),
        ],
    },
    "wind_daily": {
        "bronze_table": "wind_raw",
        "section": "daily",
        "measures": [
            ("wind_speed_max_kmh", "wind_speed_10m_max", "NUMERIC(6,2)", "km/h"),
            ("wind_gusts_max_kmh", "wind_gusts_10m_max", "NUMERIC(6,2)", "km/h"),
            ("wind_direction_dominant_deg", "wind_direction_10m_dominant", "SMALLINT", "°"),
        ],
    },
    "sunrise_sunset_daily": {
        "bronze_table": "sunrise_sunset_raw",
        "section": "daily",
        "measures": [
            ("sunrise_local", "sunrise", "TIMESTAMP", "iso8601"),
            ("sunset_local", "sunset", "TIMESTAMP", "iso8601"),
            ("daylight_duration_s", "daylight_duration", "NUMERIC(8,2)", "s"),
            ("sunshine_duration_s", "sunshine_duration", "NUMERIC(8,2)", "s"),
        ],
    },
}

# Grain columns per section: (DDL, SELECT expression over the time array element t).
GRAIN = {
    "hourly": {
        "key": "forecast_time_local",
        "columns": ["forecast_time_local", "forecast_time_utc"],
        "ddl": "forecast_time_local TIMESTAMP NOT NULL,\n    forecast_time_utc TIMESTAMPTZ NOT NULL,",
        "select": "t.val::timestamp AS forecast_time_local,\n"
        "       t.val::timestamp AT TIME ZONE (b.payload->>'timezone') AS forecast_time_utc",
    },
    "daily": {
        "key": "forecast_date",
        "columns": ["forecast_date"],
        "ddl": "forecast_date DATE NOT NULL,",
        "select": "t.val::date AS forecast_date",
    },
}

AUDIT_COLUMNS_DDL = """
    -- versioning (SCD Type 2)
    row_start_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_end_date        TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 00:00:00+00',
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,

    -- audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by          TEXT        NOT NULL DEFAULT current_user,
    row_updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by          TEXT        NOT NULL DEFAULT current_user,

    -- soft delete
    is_deleted          BOOLEAN     NOT NULL DEFAULT FALSE,
    deleted_at          TIMESTAMPTZ,
    deleted_by          TEXT,
    delete_reason       TEXT,
"""

COMMON_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};

CREATE OR REPLACE FUNCTION {SILVER_SCHEMA}.set_row_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.row_updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS {LOAD_LOG} (
    id                  BIGSERIAL PRIMARY KEY,
    silver_table        TEXT    NOT NULL,
    bronze_table        TEXT    NOT NULL,
    bronze_id           BIGINT  NOT NULL,
    rows_in_source      INTEGER NOT NULL,
    rows_inserted       INTEGER NOT NULL,
    rows_closed         INTEGER NOT NULL,
    rows_unchanged      INTEGER NOT NULL,
    dag_id              TEXT    NOT NULL,
    dag_run_id          TEXT    NOT NULL,
    task_id             TEXT    NOT NULL,
{AUDIT_COLUMNS_DDL}
    CONSTRAINT etl_load_log_row_dates_chk CHECK (row_end_date >= row_start_date),
    CONSTRAINT etl_load_log_soft_delete_chk CHECK (is_deleted = (deleted_at IS NOT NULL))
);
-- A bronze row is loaded into a given silver table at most once.
CREATE UNIQUE INDEX IF NOT EXISTS etl_load_log_bronze_uidx
    ON {LOAD_LOG} (silver_table, bronze_id) WHERE NOT is_deleted;
CREATE OR REPLACE TRIGGER etl_load_log_set_row_updated_at
    BEFORE UPDATE ON {LOAD_LOG}
    FOR EACH ROW EXECUTE FUNCTION {SILVER_SCHEMA}.set_row_updated_at();
"""


def silver_table_ddl(name: str) -> str:
    spec = SILVER_TABLES[name]
    grain = GRAIN[spec["section"]]
    measures = ",\n    ".join(f"{col} {sql_type}" for col, _, sql_type, _ in spec["measures"])
    return f"""
CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{name} (
    id                  BIGSERIAL PRIMARY KEY,

    -- grain
    location_name       TEXT NOT NULL,
    timezone            TEXT NOT NULL,
    {grain["ddl"]}

    -- measures
    {measures},

    -- lineage
    record_hash         TEXT   NOT NULL,
    source_bronze_table TEXT   NOT NULL,
    source_bronze_id    BIGINT NOT NULL,
    source_dag_run_id   TEXT   NOT NULL,
    dag_id              TEXT   NOT NULL,
    dag_run_id          TEXT   NOT NULL,
    task_id             TEXT   NOT NULL,
{AUDIT_COLUMNS_DDL}
    CONSTRAINT {name}_row_dates_chk CHECK (row_end_date >= row_start_date),
    CONSTRAINT {name}_soft_delete_chk CHECK (is_deleted = (deleted_at IS NOT NULL))
);
-- At most one live current version per location and hour/day.
CREATE UNIQUE INDEX IF NOT EXISTS {name}_current_uidx
    ON {SILVER_SCHEMA}.{name} (location_name, {grain["key"]}) WHERE is_current AND NOT is_deleted;
CREATE INDEX IF NOT EXISTS {name}_source_bronze_id_idx ON {SILVER_SCHEMA}.{name} (source_bronze_id);
CREATE OR REPLACE TRIGGER {name}_set_row_updated_at
    BEFORE UPDATE ON {SILVER_SCHEMA}.{name}
    FOR EACH ROW EXECUTE FUNCTION {SILVER_SCHEMA}.set_row_updated_at();
"""


def create_silver_tables():
    ddl = COMMON_DDL + "".join(silver_table_ddl(name) for name in SILVER_TABLES)
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(ddl)


def _stage_sql(spec: dict) -> str:
    """Flatten one bronze row's payload section into a temp table, one row per hour/day."""
    section = spec["section"]
    measure_selects = ",\n           ".join(
        f"(b.payload->'{section}'->'{field}'->>(t.ord::int - 1))::{sql_type} AS {col}"
        for col, field, sql_type, _ in spec["measures"]
    )
    measure_cols = ", ".join(col for col, _, _, _ in spec["measures"])
    return f"""
    CREATE TEMP TABLE stage ON COMMIT DROP AS
    SELECT x.*, md5(ROW({measure_cols})::text) AS record_hash
    FROM (
        SELECT b.id AS source_bronze_id,
               b.dag_run_id AS source_dag_run_id,
               b.extracted_at,
               b.location_name,
               b.payload->>'timezone' AS timezone,
               {GRAIN[section]["select"]},
               {measure_selects}
        FROM bronze.{spec["bronze_table"]} b
        CROSS JOIN LATERAL jsonb_array_elements_text(b.payload->'{section}'->'time')
            WITH ORDINALITY AS t(val, ord)
        WHERE b.id = %(bronze_id)s
    ) x
    """


def load_silver_table(name: str, dag_id: str, run_id: str, task_id: str) -> list[dict]:
    """Apply every unprocessed bronze row to silver.`name`; return per-row load stats."""
    spec = SILVER_TABLES[name]
    section = spec["section"]
    grain = GRAIN[section]
    target = f"{SILVER_SCHEMA}.{name}"
    source = f"bronze.{spec['bronze_table']}"
    actor = f"airflow:{dag_id}"
    measure_cols = [col for col, _, _, _ in spec["measures"]]
    insert_cols = ["location_name", "timezone", *grain["columns"], *measure_cols, "record_hash"]
    expected_units = {field: unit for _, field, _, unit in spec["measures"]}
    params = {"actor": actor, "silver_table": name, "dag_id": dag_id, "run_id": run_id, "task_id": task_id}

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    stats = []
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT b.id FROM {source} b
                WHERE NOT b.is_deleted
                  AND NOT EXISTS (
                      SELECT 1 FROM {LOAD_LOG} l
                      WHERE l.silver_table = %(silver_table)s AND l.bronze_id = b.id AND NOT l.is_deleted
                  )
                ORDER BY b.id
                """,
                params,
            )
            pending = [row[0] for row in cur.fetchall()]

        for bronze_id in pending:
            # One transaction per bronze row: staging, SCD2 merge and load log commit together.
            with conn, conn.cursor() as cur:
                p = {**params, "bronze_id": bronze_id}

                cur.execute(f"SELECT payload->'{section}_units' FROM {source} WHERE id = %(bronze_id)s", p)
                units = cur.fetchone()[0] or {}
                mismatched = {f: units.get(f) for f, u in expected_units.items() if units.get(f) != u}
                if mismatched:
                    raise ValueError(
                        f"{source} id={bronze_id}: unexpected units {mismatched}, expected {expected_units}"
                    )

                cur.execute(_stage_sql(spec), p)
                cur.execute("SELECT count(*) FROM stage")
                rows_in_source = cur.fetchone()[0]

                key_match = " AND ".join(
                    f"tgt.{c} = s.{c}" for c in ["location_name", grain["key"]]
                )
                # Close current versions whose values changed in this forecast.
                cur.execute(
                    f"""
                    UPDATE {target} AS tgt
                    SET is_current = FALSE, row_end_date = s.extracted_at, updated_by = %(actor)s
                    FROM stage s
                    WHERE {key_match} AND tgt.is_current AND NOT tgt.is_deleted
                      AND tgt.record_hash <> s.record_hash
                    """,
                    p,
                )
                rows_closed = cur.rowcount

                # Insert new keys and new versions of the changed ones.
                cur.execute(
                    f"""
                    INSERT INTO {target} (
                        {", ".join(insert_cols)},
                        source_bronze_table, source_bronze_id, source_dag_run_id,
                        dag_id, dag_run_id, task_id, row_start_date, created_by, updated_by
                    )
                    SELECT {", ".join(f"s.{c}" for c in insert_cols)},
                           '{source}', s.source_bronze_id, s.source_dag_run_id,
                           %(dag_id)s, %(run_id)s, %(task_id)s, s.extracted_at, %(actor)s, %(actor)s
                    FROM stage s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {target} tgt
                        WHERE {key_match} AND tgt.is_current AND NOT tgt.is_deleted
                    )
                    """,
                    p,
                )
                rows_inserted = cur.rowcount

                row_stats = {
                    "bronze_id": bronze_id,
                    "rows_in_source": rows_in_source,
                    "rows_inserted": rows_inserted,
                    "rows_closed": rows_closed,
                    "rows_unchanged": rows_in_source - rows_inserted,
                }
                cur.execute(
                    f"""
                    INSERT INTO {LOAD_LOG} (
                        silver_table, bronze_table, bronze_id, rows_in_source, rows_inserted,
                        rows_closed, rows_unchanged, dag_id, dag_run_id, task_id, created_by, updated_by
                    ) VALUES (
                        %(silver_table)s, %(bronze_table)s, %(bronze_id)s, %(rows_in_source)s,
                        %(rows_inserted)s, %(rows_closed)s, %(rows_unchanged)s,
                        %(dag_id)s, %(run_id)s, %(task_id)s, %(actor)s, %(actor)s
                    )
                    """,
                    {**p, **row_stats, "bronze_table": source},
                )
                stats.append(row_stats)
                print(f"{target} <- {source} id={bronze_id}: {json.dumps(row_stats)}")
    finally:
        conn.close()
    return stats
