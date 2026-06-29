"""
Pipeline Step: Validation (Pydantic)
Pydantic models for validating UK Carbon Intensity API responses. In pipeline, store the sanitized data in validated/ for downstream processing.

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
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, field_validator

from src import config

logger = logging.getLogger(__name__)


# --- National Intensity endpoint ---
class IntensityData(BaseModel):
    forecast: int
    actual: Optional[int] = None
    index: str

    @field_validator("index")
    @classmethod
    def validate_index(cls, value):
        valid_indices = {"very low", "low", "moderate", "high", "very high"}
        if value not in valid_indices:
            raise ValueError(f"Index must be one of {valid_indices}")
        return value


class IntensityRecord(BaseModel):
    """Validate National Intensity API record."""

    from_: str = Field(alias="from")
    to: str
    intensity: IntensityData


# --- National Generation Mix endpoint ---
class GenerationMixData(BaseModel):
    fuel: str
    perc: float

    @field_validator("perc")
    @classmethod
    def validate_perc(cls, value):
        if not (0 <= value <= 100):
            raise ValueError("Percentage must be between 0 and 100")
        return value


class GenerationMixRecord(BaseModel):
    """Validate National Generation Mix API record."""

    from_: str = Field(alias="from")
    to: str
    generationmix: list[GenerationMixData]


# --- Regional Intensity endpoint ---
class RegionalData(BaseModel):
    regionid: int
    dnoregion: str
    shortname: str
    intensity: IntensityData
    generationmix: list[GenerationMixData]


class RegionalIntensityRecord(BaseModel):
    """Validate Regional Intensity and Generation Mix API record."""

    from_: str = Field(alias="from")
    to: str
    regions: list[RegionalData]


# --- Validation Helper Functions ---
def validate_intensity_response(data: list) -> list[IntensityRecord]:
    """Validate a list of intensity records."""
    valid = []
    for i, record in enumerate(data):
        try:
            valid.append(IntensityRecord.model_validate(record))
        except Exception as e:
            logger.warning("Skipping National Intensity record %d: %s", i, e)
    return valid


def validate_generation_mix_response(data: list) -> list[GenerationMixRecord]:
    """Validate a list of generation mix records."""
    valid = []
    for i, record in enumerate(data):
        try:
            valid.append(GenerationMixRecord.model_validate(record))
        except Exception as e:
            logger.warning("Skipping National Generation Mix record %d: %s", i, e)
    return valid


def validate_regional_response(data: list) -> list[RegionalIntensityRecord]:
    """Validate a list of regional intensity records."""
    valid = []
    for i, record in enumerate(data):
        try:
            valid.append(RegionalIntensityRecord.model_validate(record))
        except Exception as e:
            logger.warning("Skipping Regional Intensity and Generation Mix record %d: %s", i, e)
    return valid


# --- Validation Pipeline Step (raw → validated) ---
# Validator to apply to each raw dataset.
DATASET_VALIDATORS = {
    config.NATIONAL_INTENSITY: validate_intensity_response,
    config.NATIONAL_GENERATION_MIX: validate_generation_mix_response,
    config.REGIONAL_INTENSITY_GENERATION_MIX: validate_regional_response,
}


def validate_date(date: str) -> None:
    """Validate raw JSON files for a day and write passing records to the validated zone.

    Raw files are read-only here; sanitized output goes to a separate ``validated``
    zone so the original API response is always preserved and reproducible.
    """
    logger.info("Validating data for %s", date)

    for dataset_name, validator in DATASET_VALIDATORS.items():
        raw_file = config.raw_path(dataset_name, date)
        if not raw_file.exists():
            logger.warning("No raw %s data for %s, skipping validation", dataset_name, date)
            continue

        with raw_file.open("r") as file_handle:
            raw_records = json.load(file_handle)

        validated_records = validator(raw_records)
        sanitized_records = [
            record.model_dump(by_alias=True)
            for record in validated_records
        ]

        validated_file = config.validated_path(dataset_name, date)
        validated_file.parent.mkdir(parents=True, exist_ok=True)
        with validated_file.open("w") as file_handle:
            json.dump(sanitized_records, file_handle, indent=4)

        logger.info(
            "%s: kept %d of %d records",
            dataset_name,
            len(sanitized_records),
            len(raw_records),
        )


def main(from_date: str, to_date: str) -> None:
    """Validate each day's raw datasets and persist the passing records."""
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        validate_date(date.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Validate raw UK Carbon Intensity datasets for a date range."
    )
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    main(from_date=args.from_date, to_date=args.to_date)
