"""Shared Airflow Datasets for data-aware (cross-DAG) scheduling.

DAGs are linked producer→consumer by the Dataset URI string, so keep it stable.
"""

from airflow.datasets import Dataset

# Produced by ingest_and_load's `load_raw` task; consumed as transform's schedule.
RAW_CARBON_LOADED = Dataset("carbon://postgres/raw_tables")
