"""Airflow DAG: lands raw Open-Meteo data in bronze (see common/open_meteo.py)."""
from common.open_meteo import make_bronze_dag

make_bronze_dag(
    dag_id="open_meteo_wind",
    table="wind_raw",
    description="Open-Meteo wind forecast -> bronze.wind_raw",
    schedule="@hourly",
    api_params={
        "hourly": "wind_speed_10m,wind_direction_10m,wind_gusts_10m",
        "daily": "wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant",
    },
)
