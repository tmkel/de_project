"""DAG: Ingest carbon intensity data and load to raw tables."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.ingestion import api_client
from src.storage import raw_loader, staging
from run_pipeline import validate_raw_data


default_args = {
    "owner": "teliang",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="ingest_and_load",
    default_args=default_args,
    description="Ingest UK carbon intensity data and load to raw PostgreSQL tables",
    schedule="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["carbon", "ingestion"],
) as dag:

    def _get_target_date(**context):
        """Use yesterday's date as the target."""
        execution_date = context["logical_date"]
        return (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    def _ingest(**context):
        target_date = _get_target_date(**context)
        api_client.main(target_date, target_date)

    def _validate(**context):
        target_date = _get_target_date(**context)
        validate_raw_data(target_date)

    def _stage(**context):
        target_date = _get_target_date(**context)
        staging.main(target_date, target_date)

    def _load(**context):
        target_date = _get_target_date(**context)
        raw_loader.main(target_date, target_date)

    ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
    validate = PythonOperator(task_id="validate", python_callable=_validate)
    stage = PythonOperator(task_id="stage", python_callable=_stage)
    load = PythonOperator(task_id="load_raw", python_callable=_load)

    ingest >> validate >> stage >> load