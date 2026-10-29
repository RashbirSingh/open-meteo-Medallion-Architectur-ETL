"""Airflow DAG: renders an HTML activity report from the gold layer.

Runs whenever gold.activity_daily is updated (dataset-triggered); writes to
/opt/airflow/reports (mounted from the host's loopiq/reports folder).
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from common.open_meteo import layer_dataset
from common.report import build_report

DAG_ID = "open_meteo_report"


@dag(
    dag_id=DAG_ID,
    description="Open-Meteo gold -> HTML activity report",
    schedule=[layer_dataset("gold", "activity_daily")],
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["open-meteo", "report"],
)
def open_meteo_report():
    @task
    def generate_report() -> str:
        return build_report(DAG_ID, get_current_context()["run_id"])

    generate_report()


open_meteo_report()
