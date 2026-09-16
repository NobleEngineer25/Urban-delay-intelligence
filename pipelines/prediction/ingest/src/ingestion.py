import csv
import requests
from pathlib import Path


BASE_URL = "https://data.cityofchicago.org/api/v3/views/wrvz-psew/query.json"

START_DATE = "2023-01-01T00:00:00"
END_DATE = "2023-02-01T00:00:00"

OUTPUT_DIR = Path("data/raw")
OUTPUT_FILE = OUTPUT_DIR / "chicago_taxi_2023_01.csv"

PAGE_SIZE = 5000


def write_rows(data, fieldnames, mode):
    with open(
        OUTPUT_FILE,
        mode,
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            extrasaction="ignore"
        )

        if mode == "w":
            writer.writeheader()

        writer.writerows(data)


def download_taxi_data():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()

    page_number = 1
    total_downloaded = 0
    fieldnames = None

    print("Starting Chicago Taxi data ingestion...")
    print(f"Period: {START_DATE} → {END_DATE}")
    print()

    while True:

        query = (
            "SELECT * "
            f"WHERE `trip_start_timestamp` >= '{START_DATE}' "
            f"AND `trip_start_timestamp` < '{END_DATE}' "
            "ORDER BY `trip_start_timestamp`"
        )

        payload = {
            "query": query,
            "page": {
                "pageNumber": page_number,
                "pageSize": PAGE_SIZE
            }
        }

        print(f"Requesting page {page_number}...")

        response = requests.post(
            BASE_URL,
            json=payload,
            timeout=120
        )

        response.raise_for_status()

        data = response.json()

        if not data:
            break

        # First batch determines the CSV structure
        if fieldnames is None:
            fieldnames = list(data[0].keys())

            print(f"Columns found: {len(fieldnames)}")

            write_rows(
                data,
                fieldnames,
                "w"
            )

        else:
            write_rows(
                data,
                fieldnames,
                "a"
            )

        total_downloaded += len(data)

        print(
            f"Downloaded {len(data):,} rows "
            f"(total: {total_downloaded:,})"
        )

        if len(data) < PAGE_SIZE:
            break

        page_number += 1

    print()
    print("Ingestion complete!")
    print(f"Total rows: {total_downloaded:,}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    download_taxi_data()