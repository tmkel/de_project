import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime, timedelta


def staged_file_exists(dataset: str, date: str) -> bool:
    return os.path.exists(f"./data/staging/{dataset}/{date}.parquet")


def get_connection() -> psycopg2.extensions.connection | None:
    load_dotenv()  # Load environment variables from .env file

    # Support both local .env and docker environment variable conventions
    user = os.getenv('DB_USER') or os.getenv('POSTGRES_USER')
    password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('DB_HOST') or os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    dbname = os.getenv('DB_NAME') or os.getenv('POSTGRES_DB')


    db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    try:
        connection = psycopg2.connect(db_url)
        print(f"{dbname}: Connection successful!")
        return connection
    except psycopg2.Error as e:
        print(f"{dbname}: Connection failed: {e}")
        return None

def load_raw_national_intensity(cursor: psycopg2.extensions.cursor, date: str) -> None:
    national_intensity_df = pd.read_parquet(f"./data/staging/national_intensity/{date}.parquet")

    for _, row in national_intensity_df.iterrows():
        cursor.execute(
            '''
            INSERT INTO raw_national_intensity (from_date, to_date, forecast, actual, intensity_index)
            VALUES (%s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                row['intensity.forecast'],
                row['intensity.actual'],
                row['intensity.index']
            )
        )

    print(f"Raw national intensity data for {date} loaded successfully into raw_national_intensity table.")


def load_raw_generation(
    cursor: psycopg2.extensions.cursor,
    date: str,
) -> None:
    generation_df = pd.read_parquet(f"./data/staging/generation/{date}.parquet")
    for _, row in generation_df.iterrows():
        cursor.execute(
            '''
            INSERT INTO raw_generation (from_date, to_date, fuel, perc)
            VALUES (%s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                row['fuel'],
                row['perc']
            )
        )
    print(f"Raw generation data for {date} loaded successfully into raw_generation table.")


def load_raw_regional_intensity(
    cursor: psycopg2.extensions.cursor,
    date: str,
) -> None:
    regional_intensity_fuel_df = pd.read_parquet(f"./data/staging/regional_intensity/{date}.parquet")

    for _, row in regional_intensity_fuel_df.iterrows():
        cursor.execute('''
            INSERT INTO raw_regional (from_date, to_date, regionid, dnoregion, shortname, forecast, intensity_index, fuel, perc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                row['regionid'],
                row['dnoregion'],
                row['shortname'],
                row['intensity.forecast'],
                row['intensity.index'],
                row['fuel'],
                row['percentage']
            )
        )
    print(f"Raw regional intensity data for {date} loaded successfully into raw_regional table.")

def load_raw_date(date: str) -> None:
    connection = get_connection()
    if not connection:
        print("Failed to connect to the database. Cannot load data.")
        return
    cursor = connection.cursor()

    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    try:
        if staged_file_exists("national_intensity", date):
            cursor.execute(
                "DELETE FROM raw_national_intensity WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_raw_national_intensity(cursor, date)
        else:
            print(f"WARNING: No staged national intensity data for {date}, skipping")

        if staged_file_exists("generation", date):
            cursor.execute(
                "DELETE FROM raw_generation WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_raw_generation(cursor, date)
        else:
            print(f"WARNING: No staged generation data for {date}, skipping")

        if staged_file_exists("regional_intensity", date):
            cursor.execute(
                "DELETE FROM raw_regional WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )
            load_raw_regional_intensity(cursor, date)
        else:
            print(f"WARNING: No staged regional intensity data for {date}, skipping")

        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error loading staging data for {date}: {e}")
    finally:
        cursor.close()
        connection.close()

def main(from_date: str, to_date: str) -> None:
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        print(f"Loading staging data for {date_str} into raw tables...")
        load_raw_date(date_str)
        print("-" * 50 + "\n")


if __name__ == "__main__":
    main("2022-01-01", "2022-01-05")
