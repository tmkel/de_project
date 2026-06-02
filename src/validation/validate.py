"""
Pydantic models for validating UK Carbon Intensity API responses.
Store after pass the validation.

Cover:
- National Intensity endpoint
- National Generation Mix endpoint
- Regional Intensity endpoints
"""

import logging
from typing import Optional
from pydantic import BaseModel, Field, field_validator

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
    """Validate one Carbon Intensity API record."""

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
    """Validate Generation Mix API record."""

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
    """Validate Regional Intensity API record."""

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
            logger.warning("Skipping Regional Intensity record %d: %s", i, e)
    return valid
