"""
Pipeline Step: Staging (JSON → Parquet)
Load data from validated JSON files and convert to Parquet format for downstream processing.

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
from pathlib import Path

import pandas as pd

from src import config

logger = logging.getLogger(__name__)


def load_json(file_path: Path) -> list:
    with file_path.open("r") as f:
        return json.load(f)


def stage_national_intensity(date: str) -> None:
    data_list = load_json(config.validated_path(config.NATIONAL_INTENSITY, date))

    if not data_list:
        logger.warning("No intensity data for %s, skipping", date)
        return None

    data_df = pd.json_normalize(data_list)
    data_df.to_parquet(config.staging_path(config.NATIONAL_INTENSITY, date), index=False)
    return None


def stage_national_generation_mix(date: str) -> None:
    data_list = load_json(config.validated_path(config.NATIONAL_GENERATION_MIX, date))

    if not data_list:
        logger.warning("No generation data for %s, skipping", date)
        return None

    data_df = pd.json_normalize(
        data_list, record_path=["generationmix"], meta=["from", "to"]
    )
    data_df.to_parquet(config.staging_path(config.NATIONAL_GENERATION_MIX, date), index=False)
    return None


def stage_regional_intensity_generation_mix(date: str) -> None:
    data_list = load_json(config.validated_path(config.REGIONAL_INTENSITY_GENERATION_MIX, date))

    if not data_list:
        logger.warning("No regional intensity data for %s, skipping", date)
        return None

    rows = []
    for records in data_list:
        for regions in records["regions"]:
            for fuel in regions["generationmix"]:
                row = {
                    "from": records["from"],
                    "to": records["to"],
                    "regionid": regions["regionid"],
                    "dnoregion": regions["dnoregion"],
                    "shortname": regions["shortname"],
                    "intensity.forecast": regions["intensity"]["forecast"],
                    "intensity.index": regions["intensity"]["index"],
                    "fuel": fuel["fuel"],
                    "percentage": fuel["perc"],
                }
                rows.append(row)
    data_df = pd.DataFrame(rows)
    data_df.to_parquet(config.staging_path(config.REGIONAL_INTENSITY_GENERATION_MIX, date), index=False)
    return None


def main(from_date: str, to_date: str) -> None:
    for dataset in config.DATASETS:
        (config.STAGING_DIR / dataset).mkdir(parents=True, exist_ok=True)
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        logger.info("Staging data for %s", date_str)
        stage_national_intensity(date_str)
        stage_national_generation_mix(date_str)
        stage_regional_intensity_generation_mix(date_str)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Stage validated UK Carbon Intensity datasets to parquet for a date range.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    main(from_date=args.from_date, to_date=args.to_date)