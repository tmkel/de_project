import json
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


def load_json(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def stage_national_intensity(date: str) -> None:
    data_list = load_json(f"./data/raw/national_intensity/{date}.json")

    if not data_list:
        logger.warning("No intensity data for %s, skipping", date)
        return None

    data_df = pd.json_normalize(data_list)
    data_df.to_parquet(f"./data/staging/national_intensity/{date}.parquet", index=False)
    return None


def stage_generation(date: str) -> None:
    data_list = load_json(f"./data/raw/generation/{date}.json")

    if not data_list:
        logger.warning("No generation data for %s, skipping", date)
        return None

    data_df = pd.json_normalize(
        data_list, record_path=["generationmix"], meta=["from", "to"]
    )
    data_df.to_parquet(f"./data/staging/generation/{date}.parquet", index=False)
    return None


def stage_regional_intensity(date: str) -> None:
    data_list = load_json(f"./data/raw/regional_intensity/{date}.json")

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
    data_df.to_parquet(f"./data/staging/regional_intensity/{date}.parquet", index=False)
    return None


def main(from_date: str, to_date: str) -> None:
    os.makedirs("./data/staging/national_intensity", exist_ok=True)
    os.makedirs("./data/staging/generation", exist_ok=True)
    os.makedirs("./data/staging/regional_intensity", exist_ok=True)
    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        logger.info("Staging data for %s", date_str)
        stage_national_intensity(date_str)
        stage_generation(date_str)
        stage_regional_intensity(date_str)


if __name__ == "__main__":
    main("2022-01-01", "2022-01-05")
