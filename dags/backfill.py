"""DAG: Backfill historical data (manual trigger only)."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion import api_client
from src.storage import raw_loader, staging
from run_pipeline import validate_raw_data

logger = logging.getLogger(__name__)


default_args = {
    "owner": "teliang",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="backfill_historical",
    default_args=default_args,
    description="Backfill historical carbon intensity data (manual trigger)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["carbon", "backfill"],
    params={
        "from_date": "2025-10-01",
        "to_date": "2026-03-31",
    },
) as dag:

    def _backfill_window(from_date: str, to_date: str):
        """Ingest, validate, stage, and load a single date window."""
        import pandas as pd

        api_client.main(from_date, to_date)

        for date in pd.date_range(from_date, to_date, freq="D"):
            validate_raw_data(date.strftime("%Y-%m-%d"))

        staging.main(from_date, to_date)
        raw_loader.main(from_date, to_date)

    def _run_backfill(**context):
        """Split the full range into 14-day windows and process each."""
        import pandas as pd

        params = context["params"]
        from_date = params["from_date"]
        to_date = params["to_date"]

        windows = pd.date_range(from_date, to_date, freq="14D")
        end = pd.Timestamp(to_date)

        for window_start in windows:
            window_end = min(window_start + timedelta(days=13), end)
            logger.info("Backfilling %s to %s", window_start.date(), window_end.date())
            _backfill_window(
                window_start.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d"),
            )

    backfill = PythonOperator(
        task_id="backfill",
        python_callable=_run_backfill,
    )