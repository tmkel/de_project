"""
Pipeline Step: Postgres raw load
Loads staged parquet files into raw PostgreSQL tables, defined in init_db.sql

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
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src import config

logger = logging.getLogger(__name__)


def staged_file_exists(dataset: str, date: str) -> bool:
    return config.staging_path(dataset, date).exists()


def get_connection() -> psycopg2.extensions.connection | None:
    load_dotenv()  # Load environment variables from .env file

    host = os.getenv("DB_HOST") or os.getenv("POSTGRES_HOST", "localhost")
    port = "5432"
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    try:
        connection = psycopg2.connect(db_url)
        logger.info("%s: Connection successful", dbname)
        return connection
    except psycopg2.Error as e:
        logger.error("%s: Connection failed: %s", dbname, e)
        return None


def load_national_intensity(cursor: psycopg2.extensions.cursor, date: str) -> None:
    national_intensity_df = pd.read_parquet(
        config.staging_path(config.NATIONAL_INTENSITY, date)
    )

    rows = [
        (
            row["from"],
            row["to"],
            row["intensity.forecast"],
            row["intensity.actual"],
            row["intensity.index"],
        )
        for _, row in national_intensity_df.iterrows()
    ]
    cursor.executemany(
        """
        INSERT INTO raw_national_intensity (from_date, to_date, intensity_forecast, intensity_actual, intensity_index)
        VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )

    logger.info(
        "Staged national intensity data for %s loaded into raw_national_intensity (%d rows)",
        date,
        len(rows),
    )


def load_national_generation_mix(
    cursor: psycopg2.extensions.cursor,
    date: str,
) -> None:
    generation_df = pd.read_parquet(
        config.staging_path(config.NATIONAL_GENERATION_MIX, date)
    )
    rows = [
        (
            row["from"],
            row["to"],
            row["fuel"],
            row["perc"],
        )
        for _, row in generation_df.iterrows()
    ]
    cursor.executemany(
        """
        INSERT INTO raw_national_generation_mix (from_date, to_date, fuel, perc)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )
    logger.info(
        "Staged national generation mix data for %s loaded into raw_national_generation_mix (%d rows)",
        date,
        len(rows),
    )


def load_regional_intensity_generation_mix(
    cursor: psycopg2.extensions.cursor,
    date: str,
) -> None:
    regional_intensity_fuel_df = pd.read_parquet(
        config.staging_path(config.REGIONAL_INTENSITY_GENERATION_MIX, date)
    )

    rows = [
        (
            row["from"],
            row["to"],
            row["regionid"],
            row["dnoregion"],
            row["shortname"],
            row["intensity.forecast"],
            row["intensity.index"],
            row["fuel"],
            row["perc"],
        )
        for _, row in regional_intensity_fuel_df.iterrows()
    ]
    cursor.executemany(
        """
        INSERT INTO raw_regional_intensity_generation_mix (from_date, to_date, regionid, dnoregion, shortname, intensity_forecast, intensity_index, fuel, perc)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    logger.info(
        "Staged regional intensity and generation mix data for %s loaded into raw_regional_intensity_generation_mix (%d rows)",
        date,
        len(rows),
    )


def load_date(date: str) -> None:
    connection = get_connection()
    if not connection:
        logger.error("Failed to connect to the database. Cannot load data.")
        return
    cursor = connection.cursor()

    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    try:
        if staged_file_exists(config.NATIONAL_INTENSITY, date):
            cursor.execute(
                "DELETE FROM raw_national_intensity WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_national_intensity(cursor, date)
        else:
            logger.warning("No staged national intensity data for %s, skipping", date)

        if staged_file_exists(config.NATIONAL_GENERATION_MIX, date):
            cursor.execute(
                "DELETE FROM raw_national_generation_mix WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_national_generation_mix(cursor, date)
        else:
            logger.warning(
                "No staged national generation mix data for %s, skipping", date
            )

        if staged_file_exists(config.REGIONAL_INTENSITY_GENERATION_MIX, date):
            cursor.execute(
                "DELETE FROM raw_regional_intensity_generation_mix WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_regional_intensity_generation_mix(cursor, date)
        else:
            logger.warning(
                "No staged regional intensity and generation mix data for %s, skipping",
                date,
            )

        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error("Error loading staging data for %s: %s", date, e)
    finally:
        cursor.close()
        connection.close()


def main(from_date: str, to_date: str) -> None:
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        logger.info("Loading staging data for %s into raw tables", date_str)
        load_date(date_str)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Load staged parquet files into raw PostgreSQL tables."
    )
    parser.add_argument(
        "--from-date", required=True, help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--to-date", required=True, help="End date in YYYY-MM-DD format."
    )
    args = parser.parse_args()
    main(args.from_date, args.to_date)
