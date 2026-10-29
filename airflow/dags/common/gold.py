"""Silver -> gold: activity recommendations from the Open-Meteo forecast.

gold.activity_hourly  every location/hour/activity scored against the rules
                      below, with the reasons an hour is unsuitable.
gold.activity_daily   per location/day/activity: can I do it, best window,
                      all suitable windows.
gold.activity_today + question views (can_i_swim_today, ...) answer for the
                      rest of today in each location's local time, computed
                      at query time so they never go stale.

Gold tables are SCD Type 2 versioned like silver: a row changes only when
its computed answer changes.
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.open_meteo import POSTGRES_CONN_ID
from common.silver import AUDIT_COLUMNS_DDL

GOLD_SCHEMA = "gold"

# Hard limits per activity; an hour must pass all of them to be suitable.
# Temperatures are "feels like" (apparent) °C. daylight_pad_min lets an
# activity start before sunrise / end after sunset.
ACTIVITIES = {
    "swim": dict(min_feels=22, max_feels=38, ideal_feels=28, max_rain_prob=20, max_precip_mm=0.0,
                 max_wind_kmh=20, max_gust_kmh=35, daylight_pad_min=0),
    "run": dict(min_feels=4, max_feels=26, ideal_feels=14, max_rain_prob=40, max_precip_mm=0.5,
                max_wind_kmh=30, max_gust_kmh=45, daylight_pad_min=30),
    "dog_walk": dict(min_feels=2, max_feels=27, ideal_feels=16, max_rain_prob=50, max_precip_mm=1.0,
                     max_wind_kmh=35, max_gust_kmh=50, daylight_pad_min=30),
    "walk": dict(min_feels=4, max_feels=30, ideal_feels=20, max_rain_prob=50, max_precip_mm=1.0,
                 max_wind_kmh=35, max_gust_kmh=55, daylight_pad_min=30),
}

HOURLY_KEYS = ["location_name", "forecast_time_local", "activity"]
HOURLY_VALUES = [
    "timezone", "forecast_date", "forecast_time_utc", "is_suitable", "score", "fail_reasons", "is_daylight",
    "temperature_c", "apparent_temperature_c", "precipitation_mm", "precipitation_probability_pct",
    "wind_speed_kmh", "wind_gusts_kmh", "sunrise_local", "sunset_local",
    "source_temperature_id", "source_rain_id", "source_wind_id", "source_sunrise_sunset_id",
]
DAILY_KEYS = ["location_name", "forecast_date", "activity"]
DAILY_VALUES = [
    "timezone", "can_do", "answer", "suitable_hours", "suitable_windows",
    "best_window_start", "best_window_end", "best_hour_local", "best_score", "main_blocker",
    "min_apparent_temperature_c", "max_apparent_temperature_c", "total_precipitation_mm",
    "max_precipitation_probability_pct", "max_wind_speed_kmh",
]


def _table_ddl(name: str, columns: str, keys: list[str]) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.{name} (
    id                  BIGSERIAL PRIMARY KEY,
    {columns}

    -- lineage
    record_hash         TEXT NOT NULL,
    dag_id              TEXT NOT NULL,
    dag_run_id          TEXT NOT NULL,
    task_id             TEXT NOT NULL,
{AUDIT_COLUMNS_DDL}
    CONSTRAINT {name}_row_dates_chk CHECK (row_end_date >= row_start_date),
    CONSTRAINT {name}_soft_delete_chk CHECK (is_deleted = (deleted_at IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS {name}_current_uidx
    ON {GOLD_SCHEMA}.{name} ({", ".join(keys)}) WHERE is_current AND NOT is_deleted;
CREATE OR REPLACE TRIGGER {name}_set_row_updated_at
    BEFORE UPDATE ON {GOLD_SCHEMA}.{name}
    FOR EACH ROW EXECUTE FUNCTION {GOLD_SCHEMA}.set_row_updated_at();
"""


HOURLY_DDL = _table_ddl(
    "activity_hourly",
    """location_name       TEXT        NOT NULL,
    timezone            TEXT        NOT NULL,
    forecast_date       DATE        NOT NULL,
    forecast_time_local TIMESTAMP   NOT NULL,
    forecast_time_utc   TIMESTAMPTZ NOT NULL,
    activity            TEXT        NOT NULL,
    is_suitable         BOOLEAN     NOT NULL,
    score               SMALLINT    NOT NULL,
    fail_reasons        TEXT[]      NOT NULL,
    is_daylight         BOOLEAN     NOT NULL,
    temperature_c                 NUMERIC(6,2),
    apparent_temperature_c        NUMERIC(6,2),
    precipitation_mm              NUMERIC(8,2),
    precipitation_probability_pct SMALLINT,
    wind_speed_kmh                NUMERIC(6,2),
    wind_gusts_kmh                NUMERIC(6,2),
    sunrise_local                 TIMESTAMP,
    sunset_local                  TIMESTAMP,
    source_temperature_id         BIGINT NOT NULL,
    source_rain_id                BIGINT NOT NULL,
    source_wind_id                BIGINT NOT NULL,
    source_sunrise_sunset_id      BIGINT NOT NULL,""",
    HOURLY_KEYS,
)

DAILY_DDL = _table_ddl(
    "activity_daily",
    """location_name       TEXT      NOT NULL,
    timezone            TEXT      NOT NULL,
    forecast_date       DATE      NOT NULL,
    activity            TEXT      NOT NULL,
    can_do              BOOLEAN   NOT NULL,
    answer              TEXT      NOT NULL,
    suitable_hours      INTEGER   NOT NULL,
    suitable_windows    TEXT[]    NOT NULL,
    best_window_start   TIMESTAMP,
    best_window_end     TIMESTAMP,
    best_hour_local     TIMESTAMP,
    best_score          SMALLINT,
    main_blocker        TEXT,
    min_apparent_temperature_c        NUMERIC(6,2),
    max_apparent_temperature_c        NUMERIC(6,2),
    total_precipitation_mm            NUMERIC(8,2),
    max_precipitation_probability_pct SMALLINT,
    max_wind_speed_kmh                NUMERIC(6,2),""",
    DAILY_KEYS,
)


def _rules_values() -> str:
    return ",\n            ".join(
        f"('{name}', {r['min_feels']}, {r['max_feels']}, {r['ideal_feels']}, {r['max_rain_prob']}, "
        f"{r['max_precip_mm']}, {r['max_wind_kmh']}, {r['max_gust_kmh']}, "
        f"make_interval(mins => {r['daylight_pad_min']}))"
        for name, r in ACTIVITIES.items()
    )


def _current(alias: str) -> str:
    return f"{alias}.is_current AND NOT {alias}.is_deleted"


HOURLY_STAGE_SQL = f"""
CREATE TEMP TABLE stage ON COMMIT DROP AS
WITH base AS (
    SELECT t.location_name, t.timezone, t.forecast_time_local::date AS forecast_date,
           t.forecast_time_local, t.forecast_time_utc,
           t.temperature_c, t.apparent_temperature_c,
           r.precipitation_mm, r.precipitation_probability_pct,
           w.wind_speed_kmh, w.wind_gusts_kmh,
           s.sunrise_local, s.sunset_local,
           t.id AS source_temperature_id, r.id AS source_rain_id,
           w.id AS source_wind_id, s.id AS source_sunrise_sunset_id
    FROM silver.temperature_hourly t
    JOIN silver.rain_hourly r
      ON r.location_name = t.location_name AND r.forecast_time_local = t.forecast_time_local AND {_current("r")}
    JOIN silver.wind_hourly w
      ON w.location_name = t.location_name AND w.forecast_time_local = t.forecast_time_local AND {_current("w")}
    JOIN silver.sunrise_sunset_daily s
      ON s.location_name = t.location_name AND s.forecast_date = t.forecast_time_local::date AND {_current("s")}
    WHERE {_current("t")}
),
rules (activity, min_feels, max_feels, ideal_feels, max_rain_prob, max_precip_mm,
       max_wind_kmh, max_gust_kmh, daylight_pad) AS (
    VALUES {_rules_values()}
),
checked AS (
    SELECT b.*, r.*,
           b.forecast_time_local >= b.sunrise_local - r.daylight_pad
           AND b.forecast_time_local + interval '1 hour' <= b.sunset_local + r.daylight_pad AS is_daylight
    FROM base b CROSS JOIN rules r
),
scored AS (
    SELECT c.*,
           array_remove(ARRAY[
               CASE WHEN apparent_temperature_c IS NULL OR precipitation_mm IS NULL
                         OR precipitation_probability_pct IS NULL OR wind_speed_kmh IS NULL
                         OR wind_gusts_kmh IS NULL THEN 'missing data' END,
               CASE WHEN NOT is_daylight THEN 'dark' END,
               CASE WHEN apparent_temperature_c < min_feels THEN 'too cold' END,
               CASE WHEN apparent_temperature_c > max_feels THEN 'too hot' END,
               CASE WHEN precipitation_probability_pct > max_rain_prob THEN 'rain likely' END,
               CASE WHEN precipitation_mm > max_precip_mm THEN 'raining' END,
               CASE WHEN wind_speed_kmh > max_wind_kmh THEN 'too windy' END,
               CASE WHEN wind_gusts_kmh > max_gust_kmh THEN 'too gusty' END
           ]::text[], NULL) AS fail_reasons,
           GREATEST(0, LEAST(100, round(
               100 - 3 * abs(coalesce(apparent_temperature_c, ideal_feels) - ideal_feels)
                   - 0.5 * coalesce(precipitation_probability_pct, 100)
                   - 2 * GREATEST(0, coalesce(wind_speed_kmh, 0) - 10)
           )))::smallint AS score
    FROM checked c
),
final AS (
    SELECT s.*, cardinality(fail_reasons) = 0 AS is_suitable FROM scored s
)
SELECT {", ".join(HOURLY_KEYS + HOURLY_VALUES)},
       -- Source ids are lineage only; a new silver version with equal values is not a change.
       md5(ROW({", ".join(c for c in HOURLY_KEYS + HOURLY_VALUES if not c.startswith("source_"))})::text)
           AS record_hash
FROM final
"""


def summary_sql(hourly_source: str, available: str) -> str:
    """Summarise hourly activity rows into one answer per location/day/activity.

    `hourly_source` selects gold.activity_hourly rows; only rows matching
    `available` count towards suitability (lets the today view skip past hours).
    """
    group = "location_name, forecast_date, activity"
    return f"""
WITH h AS ({hourly_source}),
a AS (SELECT * FROM h WHERE {available}),
islands AS (
    SELECT a.*, a.forecast_time_local - make_interval(hours => (row_number() OVER (
               PARTITION BY {group} ORDER BY forecast_time_local))::int) AS grp
    FROM a WHERE is_suitable
),
windows AS (
    SELECT {group},
           min(forecast_time_local) AS window_start,
           max(forecast_time_local) + interval '1 hour' AS window_end,
           count(*) AS hours, avg(score) AS avg_score,
           avg(apparent_temperature_c) AS avg_feels,
           max(precipitation_probability_pct) AS max_rain_prob,
           max(wind_speed_kmh) AS max_wind
    FROM islands GROUP BY {group}, grp
),
window_list AS (
    SELECT {group}, sum(hours)::int AS suitable_hours,
           array_agg(to_char(window_start, 'HH24:MI') || '-' || to_char(window_end, 'HH24:MI')
                     ORDER BY window_start) AS suitable_windows
    FROM windows GROUP BY {group}
),
best_window AS (
    SELECT DISTINCT ON ({group}) *
    FROM windows ORDER BY {group}, avg_score DESC, hours DESC, window_start
),
best_hour AS (
    SELECT DISTINCT ON ({group}) {group}, forecast_time_local AS best_hour_local, score AS best_score,
           apparent_temperature_c AS best_feels, precipitation_probability_pct AS best_rain_prob,
           wind_speed_kmh AS best_wind
    FROM a WHERE is_suitable ORDER BY {group}, score DESC, forecast_time_local
),
blocker AS (
    SELECT DISTINCT ON ({group}) {group}, reason AS main_blocker
    FROM (SELECT {group}, reason, count(*) AS n
          FROM a CROSS JOIN LATERAL unnest(fail_reasons) AS reason
          WHERE reason <> 'dark' GROUP BY {group}, reason) r
    ORDER BY {group}, n DESC, reason
),
day AS (
    SELECT {group}, max(timezone) AS timezone,
           min(apparent_temperature_c) AS min_apparent_temperature_c,
           max(apparent_temperature_c) AS max_apparent_temperature_c,
           sum(precipitation_mm) AS total_precipitation_mm,
           max(precipitation_probability_pct) AS max_precipitation_probability_pct,
           max(wind_speed_kmh) AS max_wind_speed_kmh
    FROM h GROUP BY {group}
)
SELECT d.location_name, d.forecast_date, d.activity, d.timezone,
       coalesce(wl.suitable_hours, 0) > 0 AS can_do,
       CASE WHEN wl.suitable_hours > 0 THEN
                'Yes - ' || array_to_string(wl.suitable_windows, ', ')
                || '; best around ' || to_char(bh.best_hour_local, 'HH24:MI')
                || ' (feels ' || round(bh.best_feels) || '°C, ' || bh.best_rain_prob || '% rain chance, wind '
                || round(bh.best_wind) || ' km/h)'
            ELSE 'No - ' || coalesce(bl.main_blocker, 'no suitable daylight hours left')
       END AS answer,
       coalesce(wl.suitable_hours, 0) AS suitable_hours,
       coalesce(wl.suitable_windows, '{{}}'::text[]) AS suitable_windows,
       bw.window_start AS best_window_start, bw.window_end AS best_window_end,
       bh.best_hour_local, bh.best_score, bl.main_blocker,
       d.min_apparent_temperature_c, d.max_apparent_temperature_c, d.total_precipitation_mm,
       d.max_precipitation_probability_pct, d.max_wind_speed_kmh
FROM day d
LEFT JOIN window_list wl USING (location_name, forecast_date, activity)
LEFT JOIN best_window bw USING (location_name, forecast_date, activity)
LEFT JOIN best_hour bh USING (location_name, forecast_date, activity)
LEFT JOIN blocker bl USING (location_name, forecast_date, activity)
"""


CURRENT_HOURLY = f"SELECT * FROM {GOLD_SCHEMA}.activity_hourly WHERE is_current AND NOT is_deleted"

DAILY_STAGE_SQL = f"""
CREATE TEMP TABLE stage ON COMMIT DROP AS
SELECT x.*, md5(x::text) AS record_hash
FROM ({summary_sql(CURRENT_HOURLY, "TRUE")}) x
"""

TODAY_SOURCE = (
    f"{CURRENT_HOURLY} AND forecast_date = (now() AT TIME ZONE timezone)::date"
)
TODAY_AVAILABLE = "forecast_time_local >= date_trunc('hour', now() AT TIME ZONE timezone)"

VIEWS_DDL = f"""
DROP VIEW IF EXISTS {GOLD_SCHEMA}.activity_today CASCADE;

-- Rest of today, per location and activity, in each location's local time.
CREATE VIEW {GOLD_SCHEMA}.activity_today AS
SELECT s.*, (now() AT TIME ZONE s.timezone)::timestamp(0) AS as_of_local
FROM ({summary_sql(TODAY_SOURCE, TODAY_AVAILABLE)}) s;

CREATE VIEW {GOLD_SCHEMA}.can_i_swim_today AS
SELECT location_name, forecast_date AS local_date, can_do, answer, suitable_windows, as_of_local
FROM {GOLD_SCHEMA}.activity_today WHERE activity = 'swim';

CREATE VIEW {GOLD_SCHEMA}.can_i_go_for_a_run_today AS
SELECT location_name, forecast_date AS local_date, can_do, answer, suitable_windows, as_of_local
FROM {GOLD_SCHEMA}.activity_today WHERE activity = 'run';

CREATE VIEW {GOLD_SCHEMA}.can_i_walk_my_dog_today AS
SELECT location_name, forecast_date AS local_date, can_do, answer, suitable_windows, as_of_local
FROM {GOLD_SCHEMA}.activity_today WHERE activity = 'dog_walk';

CREATE VIEW {GOLD_SCHEMA}.best_time_to_swim_today AS
SELECT location_name, forecast_date AS local_date, can_do,
       best_window_start::time AS best_from, best_window_end::time AS best_until,
       best_hour_local::time AS best_hour, best_score, answer, as_of_local
FROM {GOLD_SCHEMA}.activity_today WHERE activity = 'swim';

CREATE VIEW {GOLD_SCHEMA}.when_can_i_walk_today AS
SELECT location_name, forecast_date AS local_date, can_do, suitable_windows, suitable_hours,
       best_window_start::time AS best_from, best_window_end::time AS best_until, answer, as_of_local
FROM {GOLD_SCHEMA}.activity_today WHERE activity = 'walk';
"""

TABLES_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};

CREATE OR REPLACE FUNCTION {GOLD_SCHEMA}.set_row_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.row_updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
{HOURLY_DDL}
{DAILY_DDL}
"""


def create_gold_objects():
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(TABLES_DDL + VIEWS_DDL)


def _merge(stage_sql: str, table: str, keys: list[str], values: list[str],
           dag_id: str, run_id: str, task_id: str) -> dict:
    """Stage the freshly computed rows, then SCD2-merge them into gold.`table`."""
    target = f"{GOLD_SCHEMA}.{table}"
    cols = keys + values
    key_match = " AND ".join(f"tgt.{k} = s.{k}" for k in keys)
    p = {"actor": f"airflow:{dag_id}", "dag_id": dag_id, "run_id": run_id, "task_id": task_id}
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(stage_sql)
            cur.execute("SELECT count(*) FROM stage")
            rows_in_source = cur.fetchone()[0]
            cur.execute(
                f"""
                UPDATE {target} AS tgt
                SET is_current = FALSE, row_end_date = now(), updated_by = %(actor)s
                FROM stage s
                WHERE {key_match} AND tgt.is_current AND NOT tgt.is_deleted
                  AND tgt.record_hash <> s.record_hash
                """,
                p,
            )
            rows_closed = cur.rowcount
            cur.execute(
                f"""
                INSERT INTO {target} ({", ".join(cols)}, record_hash,
                                      dag_id, dag_run_id, task_id, created_by, updated_by)
                SELECT {", ".join(f"s.{c}" for c in cols)}, s.record_hash,
                       %(dag_id)s, %(run_id)s, %(task_id)s, %(actor)s, %(actor)s
                FROM stage s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {target} tgt
                    WHERE {key_match} AND tgt.is_current AND NOT tgt.is_deleted
                )
                """,
                p,
            )
            rows_inserted = cur.rowcount
    finally:
        conn.close()
    stats = {"table": target, "rows_in_source": rows_in_source, "rows_inserted": rows_inserted,
             "rows_closed": rows_closed, "rows_unchanged": rows_in_source - rows_inserted}
    print(stats)
    return stats


def build_activity_hourly(dag_id: str, run_id: str, task_id: str) -> dict:
    return _merge(HOURLY_STAGE_SQL, "activity_hourly", HOURLY_KEYS, HOURLY_VALUES, dag_id, run_id, task_id)


def build_activity_daily(dag_id: str, run_id: str, task_id: str) -> dict:
    return _merge(DAILY_STAGE_SQL, "activity_daily", DAILY_KEYS, DAILY_VALUES, dag_id, run_id, task_id)
