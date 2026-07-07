"""
Pipeline Step: Validation (Pydantic)
Validate raw JSON files from the UK Carbon Intensity API via Pydantic and write passing records to the validated zone.

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

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src import config

logger = logging.getLogger(__name__)


class StrictModel(BaseModel):
    """Base for all API models. Unknown fields are kept (the record still passes) but
    surfaced as a warning by ``validate_date``, so any new key the upstream API starts
    sending is noticed without dropping data. ``extra="allow"`` stores the unknown
    fields in ``model_extra`` and includes them in ``model_dump``."""

    model_config = ConfigDict(extra="allow")


# --- Intensity (shared between national and regional) ---
class IntensityBase(StrictModel):
    """Fields common to every intensity reading: a forecast and an index band."""

    forecast: int
    index: str

    @field_validator("index")
    @classmethod
    def validate_index(cls, value):
        valid_indices = {"very low", "low", "moderate", "high", "very high"}
        if value not in valid_indices:
            raise ValueError(f"Index must be one of {valid_indices}")
        return value


# --- National Intensity endpoint ---
class NationalIntensityData(IntensityBase):
    """National intensity also carries the settled measured value.

    ``actual`` is required: the national endpoint always reports the measured
    value for settled (past) half-hours. If same-day/unsettled data is ever
    ingested, the API returns ``actual: null`` and those records will be dropped
    — relax this to ``Optional[int]`` if that becomes a supported case.
    """

    actual: int


class IntensityRecord(StrictModel):
    """Validate National Intensity API record."""

    from_: str = Field(alias="from")
    to: str
    intensity: NationalIntensityData


# --- National Generation Mix endpoint ---
class GenerationMixData(StrictModel):
    fuel: str
    perc: float

    @field_validator("perc")
    @classmethod
    def validate_perc(cls, value):
        if not (0 <= value <= 100):
            raise ValueError("Percentage must be between 0 and 100")
        return value


class GenerationMixRecord(StrictModel):
    """Validate National Generation Mix API record."""

    from_: str = Field(alias="from")
    to: str
    generationmix: list[GenerationMixData]


# --- Regional Intensity endpoint ---
class RegionalIntensityData(IntensityBase):
    """Regional intensity is forecast-only; the endpoint never reports ``actual``."""


class RegionalData(StrictModel):
    regionid: int
    dnoregion: str
    shortname: str
    intensity: RegionalIntensityData
    generationmix: list[GenerationMixData]


class RegionalIntensityRecord(StrictModel):
    """Validate Regional Intensity and Generation Mix API record."""

    from_: str = Field(alias="from")
    to: str
    regions: list[RegionalData]


# --- Validation Helper Functions ---
def validate_national_intensity_response(data: list) -> list[IntensityRecord]:
    """Validate a list of intensity records."""
    valid = []
    for i, record in enumerate(data):
        try:
            valid.append(IntensityRecord.model_validate(record))
        except Exception as e:
            logger.warning("Skipping National Intensity record %d: %s", i, e)
    return valid


def validate_national_generation_mix_response(data: list) -> list[GenerationMixRecord]:
    """Validate a list of generation mix records."""
    valid = []
    for i, record in enumerate(data):
        try:
            valid.append(GenerationMixRecord.model_validate(record))
        except Exception as e:
            logger.warning("Skipping National Generation Mix record %d: %s", i, e)
    return valid


def validate_regional_intensity_generation_mix_response(data: list) -> list[RegionalIntensityRecord]:
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
    config.NATIONAL_INTENSITY: validate_national_intensity_response,
    config.NATIONAL_GENERATION_MIX: validate_national_generation_mix_response,
    config.REGIONAL_INTENSITY_GENERATION_MIX: validate_regional_intensity_generation_mix_response,
}

# A full UTC day is 48 half-hourly intervals; the API timestamps are UTC ("...Z"),
# so this count is constant (no DST shift). Fewer records means an incomplete day.
EXPECTED_DAILY_RECORDS = 48


def _collect_new_fields(model: BaseModel, found: set[str]) -> None:
    """Recursively gather ``ClassName.field`` for any field not in the schema.

    Extras are populated in ``model_extra`` because the API models allow (but flag)
    unknown fields; nested models are walked so a new key at any depth is reported.
    """
    if model.model_extra:
        found.update(f"{type(model).__name__}.{key}" for key in model.model_extra)
    for value in model.__dict__.values():
        if isinstance(value, BaseModel):
            _collect_new_fields(value, found)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, BaseModel):
                    _collect_new_fields(item, found)


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

        kept = len(sanitized_records)
        logger.info("%s: kept %d of %d records", dataset_name, kept, len(raw_records))

        # Flag (but don't drop) any field the upstream API added that isn't in the
        # schema, aggregated across the day so a new key logs once, not per record.
        new_fields: set[str] = set()
        for record in validated_records:
            _collect_new_fields(record, new_fields)
        if new_fields:
            logger.warning(
                "%s %s: upstream API returned field(s) not in schema: %s",
                dataset_name,
                date,
                ", ".join(sorted(new_fields)),
            )

        # Flag data-quality issues: records that failed the schema, and days that
        # are not the expected 48 half-hourly records.
        dropped = len(raw_records) - kept
        if dropped:
            logger.warning(
                "%s %s: %d record(s) failed validation and were dropped",
                dataset_name,
                date,
                dropped,
            )
        if kept != EXPECTED_DAILY_RECORDS:
            logger.warning(
                "%s %s: incomplete day, expected %d records but kept %d",
                dataset_name,
                date,
                EXPECTED_DAILY_RECORDS,
                kept,
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
