"""Airflow DAG: lands raw Open-Meteo data in bronze (see common/open_meteo.py)."""
from common.open_meteo import make_bronze_dag

make_bronze_dag(
    dag_id="open_meteo_temperature",
    table="temperature_raw",
    description="Open-Meteo temperature forecast -> bronze.temperature_raw",
    schedule="@hourly",
    api_params={
        "hourly": "temperature_2m,apparent_temperature,dew_point_2m",
        "daily": "temperature_2m_max,temperature_2m_min",
    },
)
