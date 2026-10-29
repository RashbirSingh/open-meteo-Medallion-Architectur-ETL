"""Airflow DAG: flattens bronze Open-Meteo JSON into typed, versioned silver tables.

Runs whenever any bronze pipeline lands new data (dataset-triggered).
"""
from datetime import timedelta

import pendulum
from airflow.datasets import DatasetAny
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from common.open_meteo import bronze_dataset, layer_dataset
from common.silver import SILVER_TABLES, create_silver_tables, load_silver_table

DAG_ID = "open_meteo_silver"
BRONZE_TABLES = sorted({spec["bronze_table"] for spec in SILVER_TABLES.values()})


@dag(
    dag_id=DAG_ID,
    description="Open-Meteo bronze -> silver (typed, SCD2-versioned)",
    schedule=DatasetAny(*(bronze_dataset(t) for t in BRONZE_TABLES)),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["open-meteo", "silver"],
)
def open_meteo_silver():
    @task
    def create_tables():
        create_silver_tables()

    @task
    def load(silver_table: str) -> list[dict]:
        context = get_current_context()
        return load_silver_table(silver_table, DAG_ID, context["run_id"], context["ti"].task_id)

    ddl = create_tables()
    for name in SILVER_TABLES:
        ddl >> load.override(task_id=f"load_{name}", outlets=[layer_dataset("silver", name)])(name)


open_meteo_silver()
