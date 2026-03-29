"""
Download raw national and regional carbon-intensity and generation mix datasets from the UK API.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

BASE_URL = "https://api.carbonintensity.org.uk"

headers = {"Accept": "application/json"}
RAW_DATA_DIRS = {
    "national_intensity": "./data/raw/national_intensity",
    "generation": "./data/raw/generation",
    "regional_intensity": "./data/raw/regional_intensity",
}


def _fetch_data(endpoint: str, context: str) -> list:
    """Fetch an API endpoint and return the response payload's `data` field."""
    try:
        response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=30)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.Timeout:
        print(f"Request timed out for {context}")
    except requests.exceptions.HTTPError as exc:
        print(f"HTTP error for {context}: {exc}")
    except Exception as exc:
        print(f"Unexpected error for {context}: {exc}")

    return []


def get_carbon_intensity_national(date: str) -> list:
    """
    Fetch national carbon intensity data for a single UTC day.

    The API returns half-hourly records for the date window, which should
    normally produce 48 data points.
    """
    return _fetch_data(
        endpoint=f"/intensity/date/{date}",
        context=date,
    )


def get_generation_mix_national(date: str) -> list:
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


def get_intensity_gm_regional(date: str) -> list:
    """
    Fetch regional intensity and generation mix data for a single UTC day.

    Like the generation endpoint, this API call uses the next day and then
    filters the response back to the requested date.
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
    print(f"Fetching data for {date}...")
    print("National Intensity...")
    intensity_data = get_carbon_intensity_national(date)
    print("National Generation Mix...")
    generation_data = get_generation_mix_national(date)
    print("Regional Intensity and Mix...")
    regional_data = get_intensity_gm_regional(date)
    print("-" * 50 + "\n")

    return {
        "national_intensity": intensity_data,
        "generation": generation_data,
        "regional_intensity": regional_data,
    }


def save_daily_datasets(
    date: str,
    datasets: dict[str, list],
    output_dirs: dict[str, str] | None = None,
) -> None:
    """Save a day's datasets as JSON files using the configured output locations."""
    target_dirs = output_dirs or RAW_DATA_DIRS

    for dataset_name, output_dir in target_dirs.items():
        os.makedirs(output_dir, exist_ok=True)
        with open(f"{output_dir}/{date}.json", "w") as file_handle:
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
    main(from_date="2022-01-01", to_date="2022-01-05")
