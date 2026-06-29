"""
Shared pipeline configuration: canonical dataset names and data-zone paths.

Dataset names:
- national_intensity
- national_generation_mix
- regional_intensity_generation_mix

Data zones (each step reads the previous zone and writes the next):
- raw/: exactly what the API returned
- validated/: pydantic-passed, sanitized records
- staging/: parquet, ready for Postgres load / S3 upload
"""

from pathlib import Path

# --- Data zones ---
DATA_DIR = Path("./data")
RAW_DIR = DATA_DIR / "raw"
VALIDATED_DIR = DATA_DIR / "validated"
STAGING_DIR = DATA_DIR / "staging"

# --- Canonical dataset names (used in folder names and S3 keys) ---
NATIONAL_INTENSITY = "national_intensity"
NATIONAL_GENERATION_MIX = "national_generation_mix"
REGIONAL_INTENSITY_GENERATION_MIX = "regional_intensity_generation_mix"

DATASETS = [
    NATIONAL_INTENSITY,
    NATIONAL_GENERATION_MIX,
    REGIONAL_INTENSITY_GENERATION_MIX,
]


def raw_path(dataset: str, date: str) -> Path:
    """Path to a raw JSON file for a dataset/day."""
    return RAW_DIR / dataset / f"{date}.json"


def validated_path(dataset: str, date: str) -> Path:
    """Path to a validated JSON file for a dataset/day."""
    return VALIDATED_DIR / dataset / f"{date}.json"


def staging_path(dataset: str, date: str) -> Path:
    """Path to a staged parquet file for a dataset/day."""
    return STAGING_DIR / dataset / f"{date}.parquet"
