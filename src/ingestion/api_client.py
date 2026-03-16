import os
import json
import time
import requests
import pandas as pd

# uv run ruff format src/
# rsync -av --delete ~/TeliangWorkSpace/de_project/ /mnt/c/Users/tmtl3/Documents/de_project/
# fucntin indentention not consistent
# common functions for error handling

BASE_URL = "https://api.carbonintensity.org.uk"

headers = {"Accept": "application/json"}

def get_intensity(date: str) -> list:
    '''
    Get national intensity data for a given date past 24 hours (pt24h).
    '''
    try:
        r = requests.get(
            f"{BASE_URL}/intensity/date/{date}", headers=headers, timeout=10
        )  # timeout error will be raised if the request takes longer than 10 seconds
        r.raise_for_status()  # status code error will be raised if the response status code is not 200
        return r.json()[
            "data"
        ]  # r.json() is dict, we want the 'data' key which is a list of dicts
    except requests.exceptions.Timeout:  # same as requests.Timeout
        print(f"Request timed out for {date}")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for {date}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error for {date}: {e}")
        return []


def get_generation(date: str) -> list:
    '''
    Get national generation mix data for a given date past 24 hours (pt24h).
    '''
    try:
        r = requests.get(
            f"{BASE_URL}/generation/{date}/pt24h", headers=headers, timeout=10
        )
        r.raise_for_status()
        return r.json()["data"]
    except requests.exceptions.Timeout:
        print(f"Request timed out for {date}")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for {date}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error for {date}: {e}")
        return []


def get_regional(date: str) -> list:
    '''
    Get regional intensity and generation mix data for a given date past 24 hours (pt24h).
    '''
    try:
        r = requests.get(f"{BASE_URL}/regional/intensity/{date}/pt24h", headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()["data"]
    except requests.exceptions.Timeout:
        print(f"Request timed out for regional data on {date}")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for regional data on {date}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error for regional data on {date}: {e}")
        return []


def main(from_date: str, to_date: str) -> None:
    # separte fetching and saving later, for easily swap
    os.makedirs("./data/raw/national_intensity", exist_ok=True)
    os.makedirs("./data/raw/generation", exist_ok=True)
    os.makedirs("./data/raw/regional_intensity", exist_ok=True)

    for date in pd.date_range(start=from_date, end=to_date, freq="D"):
        date_str = date.strftime("%Y-%m-%d")
        
        print(f"Fetching data for {date_str}...")
        print("National Intensity...")
        intensity_dict = get_intensity(date_str)
        print("National Generation Mix...")
        generation_dict = get_generation(date_str)
        print("Regional Intensity and Mix...")
        regional_dict = get_regional(date_str)
        print("-" * 50 + "\n")
        with open(f"./data/raw/national_intensity/{date_str}.json", "w") as f:
            json.dump(intensity_dict, f, indent=4)

        with open(f"./data/raw/generation/{date_str}.json", "w") as f:
            json.dump(generation_dict, f, indent=4)
        
        with open(f"./data/raw/regional_intensity/{date_str}.json", "w") as f:
            json.dump(regional_dict, f, indent=4)
        
        
        time.sleep(0.5)  # sleep for 0.5 seconds to avoid hitting the API rate limit

if __name__ == "__main__":
  main(from_date="2022-01-01", to_date="2022-01-05")