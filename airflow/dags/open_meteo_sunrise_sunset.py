"""Airflow DAG: lands raw Open-Meteo data in bronze (see common/open_meteo.py)."""
from common.open_meteo import make_bronze_dag

make_bronze_dag(
    dag_id="open_meteo_sunrise_sunset",
    table="sunrise_sunset_raw",
    description="Open-Meteo sunrise/sunset forecast -> bronze.sunrise_sunset_raw",
    schedule="@daily",
    api_params={
        "daily": "sunrise,sunset,daylight_duration,sunshine_duration",
    },
)
