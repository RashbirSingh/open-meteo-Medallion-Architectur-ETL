"""Airflow DAG: builds gold activity recommendations (swim, run, walk, dog walk) from silver.

Runs whenever the silver tables it reads are updated (dataset-triggered).
"""
from datetime import timedelta

import pendulum
from airflow.datasets import DatasetAny
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from common.gold import build_activity_daily, build_activity_hourly, create_gold_objects
from common.open_meteo import layer_dataset

DAG_ID = "open_meteo_gold"
SILVER_INPUTS = ["temperature_hourly", "rain_hourly", "wind_hourly", "sunrise_sunset_daily"]


@dag(
    dag_id=DAG_ID,
    description="Open-Meteo silver -> gold activity recommendations",
    schedule=DatasetAny(*(layer_dataset("silver", t) for t in SILVER_INPUTS)),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["open-meteo", "gold"],
)
def open_meteo_gold():
    @task
    def create_objects():
        create_gold_objects()

    @task(outlets=[layer_dataset("gold", "activity_hourly")])
    def activity_hourly() -> dict:
        context = get_current_context()
        return build_activity_hourly(DAG_ID, context["run_id"], context["ti"].task_id)

    @task(outlets=[layer_dataset("gold", "activity_daily")])
    def activity_daily() -> dict:
        context = get_current_context()
        return build_activity_daily(DAG_ID, context["run_id"], context["ti"].task_id)

    create_objects() >> activity_hourly() >> activity_daily()


open_meteo_gold()
