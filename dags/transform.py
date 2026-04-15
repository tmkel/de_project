"""DAG: Run dbt transformations after data is loaded."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)


default_args = {
    "owner": "teliang",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="transform",
    default_args=default_args,
    description="Run dbt build to transform raw data into staging and mart models",
    schedule="0 3 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["carbon", "dbt", "transform"],
) as dag:

    def _check_fresh_data(**context):
        """Verify that today's raw data exists before running dbt."""
        from src.storage.raw_loader import get_connection

        target_date = (context["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")
        conn = get_connection()
        if conn is None:
            raise RuntimeError("Could not connect to the database for freshness check.")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT count(*) FROM raw_national_intensity WHERE from_date >= %s",
            (f"{target_date}T00:00Z",),
        )
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if count == 0:
            raise ValueError(f"No raw data found for {target_date}. Ingest may not have run.")
        logger.info("Found %d rows for %s, proceeding with dbt.", count, target_date)

    check_data = PythonOperator(
        task_id="check_fresh_data",
        python_callable=_check_fresh_data,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/uk_carbon && dbt build",
    )

    check_data >> dbt_build