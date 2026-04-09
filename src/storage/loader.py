import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime, timedelta


def staged_file_exists(dataset: str, date: str) -> bool:
    return os.path.exists(f"./data/staging/{dataset}/{date}.parquet")


def get_connection() -> psycopg2.extensions.connection | None:
    load_dotenv()  # Load environment variables from .env file
    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    try:
        connection = psycopg2.connect(db_url)
        print(f"{os.getenv('DB_NAME')}: Connection successful!")
        return connection
    except psycopg2.Error as e:
        print(f"{os.getenv('DB_NAME')}: Connection failed: {e}")
        return None


def preload_fuel_lookup(cursor: psycopg2.extensions.cursor) -> dict[str, int]:
    cursor.execute("SELECT fuel, fuel_id FROM dim_fuel")
    return {fuel: fuel_id for fuel, fuel_id in cursor.fetchall()}


def preload_region_lookup(cursor: psycopg2.extensions.cursor) -> dict[tuple[int, str, str], int]:
    cursor.execute("SELECT api_region_id, dnoregion, shortname, region_id FROM dim_region")
    return {
        (api_region_id, dnoregion, shortname): region_id
        for api_region_id, dnoregion, shortname, region_id in cursor.fetchall()
    }


def ensure_fuel(cursor: psycopg2.extensions.cursor, fuel_lookup: dict[str, int], fuel_name: str) -> int:
    """Insert fuel if it doesn't exist, return its fuel_id."""
    if fuel_name in fuel_lookup:
        return fuel_lookup[fuel_name]

    cursor.execute(
        """
        INSERT INTO dim_fuel (fuel, category)
        VALUES (%s, %s)
        ON CONFLICT (fuel) DO NOTHING
        RETURNING fuel_id
        """,
        (fuel_name, 'other'),
    )
    inserted_row = cursor.fetchone()
    if inserted_row:
        print(f"New fuel inserted into dim_fuel: {fuel_name}")
        fuel_lookup[fuel_name] = inserted_row[0]
        return inserted_row[0]


def ensure_region(
    cursor: psycopg2.extensions.cursor,
    region_lookup: dict[tuple[int, str, str], int],
    region_info: tuple[int, str, str],
) -> int:
    """Insert region if it doesn't exist, return its region_id."""
    api_region_id, dnoregion, shortname = region_info
    if region_info in region_lookup:
        return region_lookup[region_info]

    cursor.execute(
        """
        INSERT INTO dim_region (api_region_id,dnoregion,shortname)
        VALUES (%s, %s, %s)
        ON CONFLICT (dnoregion) DO NOTHING
        RETURNING region_id
        """,
        (api_region_id, dnoregion, shortname),
    )
    inserted_row = cursor.fetchone()
    if inserted_row:
        print(f"New region inserted into dim_region: {dnoregion} ({shortname})")
        region_lookup[region_info] = inserted_row[0]
        return inserted_row[0]

    raise RuntimeError(
        "Failed to insert region and no existing region_id was found for "
        f"dnoregion={dnoregion!r}, shortname={shortname!r}, api_region_id={api_region_id!r}."
    )


def load_national_intensity(cursor: psycopg2.extensions.cursor, date: str) -> None:
    if not staged_file_exists("national_intensity", date):
        print(f"WARNING: No staged national intensity data for {date}, skipping")
        return None
    national_intensity_df = pd.read_parquet(f"./data/staging/national_intensity/{date}.parquet")

    for _, row in national_intensity_df.iterrows():
        cursor.execute(
            '''
            INSERT INTO fact_intensity (from_date, to_date, region_id, forecast, actual, intensity_index, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                1,
                row['intensity.forecast'],
                row['intensity.actual'],
                row['intensity.index'],
                f"national_intensity"
            )
        )

    print(f"National intensity data for {date} loaded successfully into fact_intensity table.")

def load_generation(
    cursor: psycopg2.extensions.cursor,
    fuel_lookup: dict[str, int],
    date: str,
) -> None:
    if not staged_file_exists("generation", date):
        print(f"WARNING: No staged generation data for {date}, skipping")
        return None
    generation_df = pd.read_parquet(f"./data/staging/generation/{date}.parquet")
    for _, row in generation_df.iterrows():
        cursor.execute(
            '''
            INSERT INTO fact_generation (from_date, to_date, region_id, fuel_id, perc, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                1,  # National level is represented by region_id = 1
                ensure_fuel(cursor, fuel_lookup, row['fuel']),
                row['perc'],
                f"generation_intensity"
            )
        )
    print(f"Generation data for {date} loaded successfully into fact_generation table.")


def load_regional_intensity(
    cursor: psycopg2.extensions.cursor,
    fuel_lookup: dict[str, int],
    region_lookup: dict[tuple[int, str, str], int],
    date: str,
) -> None:
    if not staged_file_exists("regional_intensity", date):
        print(f"WARNING: No staged regional intensity data for {date}, skipping")
        return None

    regional_intensity_fuel_df = pd.read_parquet(f"./data/staging/regional_intensity/{date}.parquet")
    regional_intensity_df = regional_intensity_fuel_df.drop(columns=['fuel', 'percentage']).drop_duplicates()

    for _, row in regional_intensity_df.iterrows():
        region_info = (row['regionid'], row['dnoregion'], row['shortname'])
        region_id = ensure_region(cursor, region_lookup, region_info)

        cursor.execute(
            '''
            INSERT INTO fact_intensity (from_date, to_date, region_id, forecast, actual, intensity_index, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                region_id,
                row['intensity.forecast'],
                None,
                row['intensity.index'],
                f"regional_intensity"
            )
        )

    for _, row in regional_intensity_fuel_df.iterrows():
        region_info = (row['regionid'], row['dnoregion'], row['shortname'])
        region_id = ensure_region(cursor, region_lookup, region_info)

        cursor.execute('''
            INSERT INTO fact_generation (from_date, to_date, region_id, fuel_id, perc, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ''',
            (
                row['from'],
                row['to'],
                region_id,
                ensure_fuel(cursor, fuel_lookup, row['fuel']),
                row['percentage'],
                f"regional_intensity"
            )
        )
    print(f"Regional intensity data for {date} loaded successfully into fact_intensity and fact_generation tables.")

def load_date(date: str) -> None:
    connection = get_connection()
    if not connection:
        print("Failed to connect to the database. Cannot load data.")
        return
    cursor = connection.cursor()
    
    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    try:
        fuel_lookup = preload_fuel_lookup(cursor)
        region_lookup = preload_region_lookup(cursor)

        if staged_file_exists("national_intensity", date) or staged_file_exists("regional_intensity", date):
            cursor.execute(
                "DELETE FROM fact_intensity WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )

        if staged_file_exists("generation", date) or staged_file_exists("regional_intensity", date):
            cursor.execute(
                "DELETE FROM fact_generation WHERE from_date >= %s AND from_date < %s",
                (f"{date}T00:00Z", f"{next_date}T00:00Z"),
            )

        load_national_intensity(cursor, date)
        load_generation(cursor, fuel_lookup, date)
        load_regional_intensity(cursor, fuel_lookup, region_lookup, date)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error loading data for {date}: {e}")
    finally:
        cursor.close()
        connection.close()

def main(from_date: str, to_date: str) -> None:
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        print(f"Loading data for {date_str}...")
        load_date(date_str)
        print("-" * 50 + "\n")


if __name__ == "__main__":
    main("2022-01-01", "2022-01-05")
