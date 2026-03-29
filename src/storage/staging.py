import json
import os
import pandas as pd


def load_json(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def stage_national_intensity(date: str) -> None:
    data_list = load_json(f"./data/raw/national_intensity/{date}.json")

    if not data_list:
        print(f"WARNING: No intensity data for {date}, skipping")
        return None

    data_df = pd.json_normalize(data_list)
    data_df.to_parquet(f"./data/staging/national_intensity/{date}.parquet", index=False)
    return None


def stage_generation(date: str) -> None:
    data_list = load_json(f"./data/raw/generation/{date}.json")

    if not data_list:
        print(f"WARNING: No generation data for {date}, skipping")
        return None

    data_df = pd.json_normalize(
        data_list, record_path=["generationmix"], meta=["from", "to"]
    )
    data_df.to_parquet(f"./data/staging/generation/{date}.parquet", index=False)
    return None


def stage_regional_intensity(date: str) -> None:
    data_list = load_json(f"./data/raw/regional_intensity/{date}.json")

    if not data_list:
        print(f"WARNING: No regional intensity data for {date}, skipping")
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
        print(f"Staging data for {date_str}...")
        stage_national_intensity(date_str)
        stage_generation(date_str)
        stage_regional_intensity(date_str)
        print("-" * 50 + "\n")


if __name__ == "__main__":
    main("2022-01-01", "2022-01-05")
