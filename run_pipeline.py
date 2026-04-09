"""Single entry point for the ingest -> validate -> stage -> load workflow."""

from __future__ import annotations

import subprocess
import argparse
import json
from pathlib import Path

import pandas as pd

from src.ingestion import api_client
from src.models.schemas import (
    validate_generation_mix_response,
    validate_intensity_response,
    validate_regional_response,
)
from src.storage import raw_loader, staging

RAW_DATASET_CONFIG = {
    "national_intensity": {
        "path": Path("./data/raw/national_intensity"),
        "validator": validate_intensity_response,
    },
    "generation": {
        "path": Path("./data/raw/generation"),
        "validator": validate_generation_mix_response,
    },
    "regional_intensity": {
        "path": Path("./data/raw/regional_intensity"),
        "validator": validate_regional_response,
    },
}


def _date_strings(from_date: str, to_date: str) -> list[str]:
    """Return inclusive date strings for the requested range."""
    return [date.strftime("%Y-%m-%d") for date in pd.date_range(from_date, to_date, freq="D")]


def validate_raw_data(date: str) -> None:
    """Validate saved raw JSON files and persist only records that pass validation."""
    print(f"Validating data for {date}...")

    for dataset_name, config in RAW_DATASET_CONFIG.items():
        file_path = config["path"] / f"{date}.json"
        if not file_path.exists():
            print(f"WARNING: No raw {dataset_name} data for {date}, skipping validation")
            continue

        with file_path.open("r") as file_handle:
            raw_records = json.load(file_handle)

        validated_records = config["validator"](raw_records)
        sanitized_records = [
            record.model_dump(by_alias=True)
            for record in validated_records
        ]

        with file_path.open("w") as file_handle:
            json.dump(sanitized_records, file_handle, indent=4)

        print(
            f"{dataset_name}: kept {len(sanitized_records)} of {len(raw_records)} records"
        )

    print("-" * 50 + "\n")


def run_pipeline(
    from_date: str,
    to_date: str,
    *,
    skip_ingest: bool = False,
    skip_load: bool = False,
    skip_transform: bool = False,
) -> None:
    """Run the pipeline steps in order, with optional ingest/load skips."""
    if not skip_ingest:
        print("Starting ingestion...")
        api_client.main(from_date, to_date)
    else:
        print("Skipping ingestion and using existing raw files.")

    for date in _date_strings(from_date, to_date):
        validate_raw_data(date)

    print("Starting staging...")
    staging.main(from_date, to_date)

    if not skip_load:
        print("Starting load...")
        raw_loader.main(from_date, to_date)
    else:
        print("Skipping load.")

    if not skip_load and not skip_transform:
        print("Starting dbt transform...")
        dbt_dir = Path(__file__).parent / "uk_carbon"

        result = subprocess.run(
            ["dbt", "build"],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"dbt build failed:\n{result.stderr}")
            raise SystemExit(1)
    else:
        print("Skipping dbt transform.")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the pipeline entry point."""
    parser = argparse.ArgumentParser(
        description="Run the data pipeline from ingestion through loading."
    )
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Reuse existing raw JSON files and start from validation.",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Stop after staging without loading into the database.",
    )
    parser.add_argument(
        "--skip-transform",
        action="store_true",
        help="Skip dbt transformation step.",
    )
    return parser


def main() -> None:
    """Parse CLI arguments and execute the pipeline."""
    args = build_parser().parse_args()
    run_pipeline(
        args.from_date,
        args.to_date,
        skip_ingest=args.skip_ingest,
        skip_load=args.skip_load,
        skip_transform=args.skip_transform,
    )


if __name__ == "__main__":
    main()
