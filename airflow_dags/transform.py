"""DAG: Run dbt transformations, triggered when raw data is loaded.

Scheduled on the RAW_CARBON_LOADED dataset instead of a cron: it runs automatically
whenever ingest_and_load's `load_raw` task succeeds — i.e. daily, right after the load.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from carbon_datasets import RAW_CARBON_LOADED

default_args = {
    "owner": "teliang",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="transform",
    default_args=default_args,
    description="Run dbt build after raw data is loaded (triggered by RAW_CARBON_LOADED)",
    schedule=[RAW_CARBON_LOADED],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["carbon", "dbt", "transform"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/uk_carbon && dbt build",
    )
