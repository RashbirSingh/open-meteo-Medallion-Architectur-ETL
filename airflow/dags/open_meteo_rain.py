"""Airflow DAG: lands raw Open-Meteo data in bronze (see common/open_meteo.py)."""
from common.open_meteo import make_bronze_dag

make_bronze_dag(
    dag_id="open_meteo_rain",
    table="rain_raw",
    description="Open-Meteo rain/precipitation forecast -> bronze.rain_raw",
    schedule="@hourly",
    api_params={
        "hourly": "precipitation,rain,showers,precipitation_probability",
        "daily": "precipitation_sum,rain_sum,showers_sum,precipitation_hours,precipitation_probability_max",
    },
)
