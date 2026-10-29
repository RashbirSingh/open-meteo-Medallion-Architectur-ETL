"""Gold -> HTML report: a self-contained page answering "can I swim / run / walk today?".

Queries the current, non-deleted gold rows, embeds them as JSON in
report_template.html and writes a timestamped report plus latest.html.
"""
import json
from decimal import Decimal
from pathlib import Path

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

from common.gold import ACTIVITIES, GOLD_SCHEMA
from common.open_meteo import POSTGRES_CONN_ID

REPORT_DIR = Path("/opt/airflow/reports")
TEMPLATE = Path(__file__).with_name("report_template.html")
KEEP_REPORTS = 48
ACTIVITY_LABELS = {"swim": "Swim", "run": "Run", "dog_walk": "Walk the dog", "walk": "Walk"}

CURRENT = "is_current AND NOT is_deleted"
LOCAL_NOW_HOUR = "date_trunc('hour', now() AT TIME ZONE timezone)"

QUERIES = {
    "today": f"""
        SELECT location_name, activity, can_do, answer, suitable_windows, main_blocker,
               to_char(best_hour_local, 'HH24:MI') AS best_hour,
               to_char(as_of_local, 'YYYY-MM-DD HH24:MI') AS as_of_local
        FROM {GOLD_SCHEMA}.activity_today
    """,
    "hourly": f"""
        SELECT location_name, activity,
               to_char(forecast_time_local, 'YYYY-MM-DD"T"HH24:MI') AS t,
               is_suitable AS ok, score, fail_reasons AS reasons,
               apparent_temperature_c AS feels, precipitation_probability_pct AS rain_prob,
               precipitation_mm AS precip, wind_speed_kmh AS wind, wind_gusts_kmh AS gusts
        FROM {GOLD_SCHEMA}.activity_hourly
        WHERE {CURRENT}
          AND forecast_time_local >= {LOCAL_NOW_HOUR}
          AND forecast_time_local < {LOCAL_NOW_HOUR} + interval '48 hours'
        ORDER BY location_name, forecast_time_local, activity
    """,
    "daily": f"""
        SELECT location_name, activity, forecast_date::text AS date, can_do,
               suitable_hours AS hours, answer
        FROM {GOLD_SCHEMA}.activity_daily
        WHERE {CURRENT} AND forecast_date >= (now() AT TIME ZONE timezone)::date
        ORDER BY location_name, forecast_date, activity
    """,
    "meta": f"""
        SELECT to_char(max(row_updated_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS gold_as_of_utc
        FROM {GOLD_SCHEMA}.activity_daily WHERE {CURRENT}
    """,
}


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def build_report(dag_id: str, run_id: str) -> str:
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            results = {}
            for name, sql in QUERIES.items():
                cur.execute(sql)
                results[name] = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if not results["today"]:
        raise ValueError("gold.activity_today is empty - has the gold DAG run?")

    cities = sorted({r["location_name"] for r in results["today"]})
    if "Melbourne" in cities:
        cities.insert(0, cities.pop(cities.index("Melbourne")))

    now = pendulum.now("UTC")
    data = {
        "meta": {
            **results["meta"][0],
            "generated_at_utc": now.format("YYYY-MM-DD HH:mm"),
            "dag_id": dag_id,
            "dag_run_id": run_id,
        },
        "activities": [{"key": k, "label": ACTIVITY_LABELS.get(k, k)} for k in ACTIVITIES],
        "cities": cities,
        "today": results["today"],
        "hourly": results["hourly"],
        "daily": results["daily"],
    }
    # Escape "</" so the JSON cannot close its <script> tag.
    payload = json.dumps(data, default=_json_default, ensure_ascii=False).replace("</", "<\\/")
    html = TEMPLATE.read_text(encoding="utf-8").replace("__REPORT_DATA__", payload)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / f"activity_report_{now.format('YYYYMMDDTHHmmss')}Z.html"
    report_path.write_text(html, encoding="utf-8")
    (REPORT_DIR / "latest.html").write_text(html, encoding="utf-8")

    for old in sorted(REPORT_DIR.glob("activity_report_*.html"))[:-KEEP_REPORTS]:
        old.unlink()

    print(f"Wrote {report_path} ({len(html):,} bytes) and latest.html")
    return str(report_path)
