"""
Pipeline Entry Point
src.ingest
    ingest_api.py, 
src.validate
    validate.py
stage -> load workflow."""

from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from src.ingestion import ingest_api
from src.staging import staging
from src.storage import psql_loader
from src.validation import validate

logger = logging.getLogger(__name__)


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
        logger.info("Starting ingestion")
        ingest_api.main(from_date, to_date)
    else:
        logger.info("Skipping ingestion and using existing raw files")

    logger.info("Starting validation")
    validate.main(from_date, to_date)

    logger.info("Starting staging")
    staging.main(from_date, to_date)

    if not skip_load:
        logger.info("Starting load")
        psql_loader.main(from_date, to_date)
    else:
        logger.info("Skipping load")

    if not skip_load and not skip_transform:
        logger.info("Starting dbt transform")
        dbt_dir = Path(__file__).parent / "uk_carbon"

        result = subprocess.run(
            ["dbt", "build"],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            logger.info(result.stdout)
        if result.returncode != 0:
            logger.error("dbt build failed:\n%s", result.stderr)
            raise SystemExit(1)
    else:
        logger.info("Skipping dbt transform")


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
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
