"""
Pipeline Step: Ingestion (Python + requests)
Download data from the UK API

Covers:
- /intensity/date/{date}: national_intensity
- /generation/{from}/pt24h: national_generation_mix
- /regional/intensity/{from}/pt24h: regional_intensity_generation_mix

Ref: Overall data flow

                     UK Carbon Intensity API
                            |
                     Ingestion (Python + requests)
                            |
                     Validation (Pydantic)
                            |
                     Staging (JSON → Parquet)
                      /                    \
        Postgres raw load            S3 raw  (s3_loader)
              |                              |
    dbt  (stg → core → marts)        Glue PySpark jobs
              |                              |
      Postgres marts                 S3 curated  →  (Iceberg → Athena · planned)

"""

import argparse
import json
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from src import config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.carbonintensity.org.uk"

headers = {"Accept": "application/json"}


def _fetch_data(endpoint: str, context: str) -> list:
    """Fetch an API endpoint and return the response payload's `data` field."""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.Timeout:
        logger.error("Request timed out for %s", context)
    except requests.exceptions.HTTPError as exc:
        logger.error("HTTP error for %s: %s", context, exc)
    except Exception as exc:
        logger.error("Unexpected error for %s: %s", context, exc)

    return []


def get_national_intensity(date: str) -> list:
    """
    Fetch national carbon intensity data for a single UTC day.

    The API returns half-hourly records for the date window, which should
    normally produce 48 data points.
    """
    return _fetch_data(
        endpoint=f"/intensity/date/{date}",
        context=date,
    )


def get_national_generation_mix(date: str) -> list:
    """
    Fetch national generation mix data for a single UTC day.

    This endpoint is queried using the next day and then filtered back to the
    requested date because of how the upstream API exposes its 24-hour window.
    """
    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    data = _fetch_data(
        endpoint=f"/generation/{next_date}/pt24h",
        context=date,
    )
    # Keep only records that belong to the requested day.
    data = [r for r in data if r["from"].startswith(date)]
    return data


def get_regional_intensity_generation_mix(date: str) -> list:
    """
    Fetch regional intensity and generation mix data for a single UTC day.

    This endpoint is queried using the next day and then filtered back to the
    requested date because of how the upstream API exposes its 24-hour window.
    """
    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    data = _fetch_data(
        endpoint=f"/regional/intensity/{next_date}/pt24h",
        context=f"regional data on {date}",
    )
    # Keep only records that belong to the requested day.
    data = [r for r in data if r["from"].startswith(date)]
    return data


def fetch_daily_datasets(date: str) -> dict[str, list]:
    """Fetch all raw datasets for a single day and return them by dataset name."""
    logger.info("Fetching data for %s", date)
    logger.info("National Intensity...")
    intensity_data = get_national_intensity(date)
    logger.info("National Generation Mix...")
    generation_data = get_national_generation_mix(date)
    logger.info("Regional Intensity and Generation Mix...")
    regional_data = get_regional_intensity_generation_mix(date)

    return {
        config.NATIONAL_INTENSITY: intensity_data,
        config.NATIONAL_GENERATION_MIX: generation_data,
        config.REGIONAL_INTENSITY_GENERATION_MIX: regional_data,
    }


def save_daily_datasets(date: str, datasets: dict[str, list]) -> None:
    """Save a day's datasets as raw JSON files, one per dataset."""
    for dataset_name in config.DATASETS:
        output_path = config.raw_path(dataset_name, date)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as file_handle:
            json.dump(datasets.get(dataset_name, []), file_handle, indent=4)


def main(from_date: str, to_date: str) -> None:
    """Fetch each day's datasets and persist them through a swappable save step."""
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        datasets = fetch_daily_datasets(date_str)
        save_daily_datasets(date_str, datasets)

        # Add a short pause between daily requests to be polite to the API.
        time.sleep(0.5)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Fetch UK Carbon Intensity datasets for a date range.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    main(from_date=args.from_date, to_date=args.to_date)
