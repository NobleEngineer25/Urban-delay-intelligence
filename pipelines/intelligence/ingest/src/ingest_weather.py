"""Download daily weather observations for Chicago from NOAA NCEI.

No API key needed. Feeds Pipeline 1 as numeric features, not documents.
Output: data/raw/chicago_weather_YYYY_MM.csv
Run from the repo root:
    python pipelines/intelligence/ingest/src/ingest_weather.py
"""
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
assert (PROJECT_ROOT / ".git").exists(), f"Wrong project root: {PROJECT_ROOT}"

BASE_URL = "https://www.ncei.noaa.gov/access/services/data/v1"

# Same month as the other sources. END is exclusive, like the other scripts.
START = date(2023, 1, 1)
END = date(2023, 2, 1)

# Two airports give a rough north/south split of the city.
STATIONS = {
    "USW00094846": "Chicago O'Hare",
    "USW00014819": "Chicago Midway",
}

# Max/min temperature (°C), precipitation (mm), snowfall (mm),
# snow depth (mm), average wind speed (m/s).
DATA_TYPES = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]

OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / f"chicago_weather_{START:%Y_%m}.csv"


def fetch_weather() -> pd.DataFrame:
    params = {
        "dataset": "daily-summaries",
        "stations": ",".join(STATIONS),
        "dataTypes": ",".join(DATA_TYPES),
        "startDate": START.isoformat(),
        "endDate": (END - timedelta(days=1)).isoformat(),  # NCEI's endDate is inclusive
        "units": "metric",
        "includeStationLocation": "1",
        "format": "csv",
    }
    response = requests.get(BASE_URL, params=params, timeout=120)
    response.raise_for_status()
    if not response.text.strip():
        raise SystemExit("Empty response. Check the station IDs and dates.")
    return pd.read_csv(StringIO(response.text))


def report(df: pd.DataFrame) -> None:
    expected_days = (END - START).days
    print("\n=== COMPLETENESS ===")
    print(f"Expected days per station: {expected_days}")

    for station_id, name in STATIONS.items():
        station = df[df["STATION"] == station_id]
        print(f"\n{name} ({station_id}): {len(station)} days returned")
        if station.empty:
            print("  WARNING: no data. The station ID may be wrong.")
            continue
        for col in DATA_TYPES:
            if col not in df.columns:
                print(f"  {col}: column missing entirely")
            else:
                missing = expected_days - station[col].notna().sum()
                print(f"  {col}: {missing} days missing")

    print("\n=== SANITY CHECK (should look like a Chicago January) ===")
    for col, label in (("TMIN", "Coldest TMIN (°C)"), ("PRCP", "Total precipitation (mm)"),
                       ("SNOW", "Total snowfall (mm)")):
        if col in df.columns:
            value = df[col].min() if col == "TMIN" else df[col].sum()
            print(f"{label}: {value:.1f}")
    if "SNOW" in df.columns:
        snow_days = df.loc[df["SNOW"] > 0, "DATE"].nunique()
        print(f"Days with snowfall at either station: {snow_days}")


def main() -> None:
    print("Starting NOAA weather ingestion...")
    print(f"Period: {START} to {END} (exclusive)")
    df = fetch_weather()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(df):,} rows to {OUTPUT_FILE}")
    report(df)


if __name__ == "__main__":
    main()