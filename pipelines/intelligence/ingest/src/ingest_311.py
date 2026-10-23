"""Download one month of road-relevant Chicago 311 requests, then check geocoding.

Raw output: data/raw/chicago_311_2023_01.csv
Run from the repo root:
    python pipelines/intelligence/ingest/src/ingest_311.py
"""
import csv
import os
import time
from datetime import date, timedelta
from pathlib import Path

from duckdb import df
import pandas as pd
import requests

# Resolve paths from the repo root, so it works from any folder.
PROJECT_ROOT = Path(__file__).resolve().parents[4]
assert (PROJECT_ROOT / ".git").exists(), f"Wrong project root: {PROJECT_ROOT}"

BASE_URL = "https://data.cityofchicago.org/resource/v6vf-nfxy.json"

# Same month as the taxi data (Mohammed's ingestion.py). END is exclusive.
START = date(2023, 1, 1)
END = date(2023, 2, 1)

OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "chicago_311_2023_01.csv"

# Road-relevant types, from the Notion Docs entry.
# Strings must match the data exactly. The report below flags any type with 0 rows.
ROAD_TYPES = [
    "Pothole in Street Complaint",
    "Street Light Out Complaint",
    "Traffic Signal Out Complaint",
    "Sign Repair Request - All Other Signs",
    "Water On Street Complaint",
    "Alley Pothole Complaint",
    "Sidewalk Inspection Request",
    "Inspect Public Way Request",
    "Open Fire Hydrant Complaint",
    "Ice and Snow Removal Request",
    "Sewer Cave-In Inspection Request",
    "Street Cleaning Request",
    "Sign Repair Request - Stop Sign",
    "Street Light Pole Damage Complaint",
    "Snow – Uncleared Sidewalk Complaint",  # en dash, as in the data
]

LIMIT = 50_000  # max rows per request. One day is far below this.

# Rough Chicago bounding box, to catch bad geocodes like (0, 0).
LAT_RANGE = (41.6, 42.1)
LON_RANGE = (-88.0, -87.5)


def fetch_day(day: date, session: requests.Session) -> list[dict]:
    """One request per day. No paging needed, so no skipped or repeated rows."""
    types_sql = ", ".join("'" + t.replace("'", "''") + "'" for t in ROAD_TYPES)
    next_day = day + timedelta(days=1)
    where = (
        f"created_date >= '{day.isoformat()}T00:00:00' "
        f"AND created_date < '{next_day.isoformat()}T00:00:00' "
        f"AND sr_type in({types_sql})"
    )
    params = {"$where": where, "$limit": LIMIT, "$order": "created_date"}

    response = session.get(BASE_URL, params=params, timeout=120)
    response.raise_for_status()
    rows = response.json()

    if len(rows) >= LIMIT:
        print(f"  WARNING: {day} hit the {LIMIT:,} row limit. Rows may be missing.")
    return rows


def write_csv(rows: list[dict]) -> None:
    """Socrata omits null fields, so build columns from ALL rows, not just the first."""
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns, restval="")
        writer.writeheader()
        writer.writerows(rows)


def geocoding_report(path: Path) -> None:
    df = pd.read_csv(path, low_memory=False)

    for col in ("latitude", "longitude", "sr_type"):
        if col not in df.columns:
            raise SystemExit(f"Column '{col}' not found.\nColumns are: {list(df.columns)}")

    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")
    has_coords = lat.notna() & lon.notna()
    in_chicago = has_coords & lat.between(*LAT_RANGE) & lon.between(*LON_RANGE)

    total = len(df)
    pct_coords = has_coords.mean() * 100
    pct_valid = in_chicago.mean() * 100

    print("\n=== GEOCODING COMPLETENESS ===")
    print(f"Total rows:                      {total:,}")
    print(f"Have latitude + longitude:       {has_coords.sum():,}  ({pct_coords:.1f}%)")
    print(f"Coordinates inside Chicago box:  {in_chicago.sum():,}  ({pct_valid:.1f}%)")

    by_type = (
        pd.DataFrame({"sr_type": df["sr_type"], "has_coords": has_coords, "valid": in_chicago})
        .groupby("sr_type")
        .agg(rows=("sr_type", "size"), pct_coords=("has_coords", "mean"), pct_valid=("valid", "mean"))
        .sort_values("rows", ascending=False)
    )
    by_type[["pct_coords", "pct_valid"]] *= 100
    print("\nBy request type:")
    print(by_type.round(1).to_string())

    missing = [t for t in ROAD_TYPES if t not in set(df["sr_type"])]

    if "duplicate" in df.columns:
            is_dup = df["duplicate"].astype(str).str.lower().eq("true")
            split = (
                pd.DataFrame({"duplicate": is_dup, "has_coords": has_coords, "valid": in_chicago})
                .groupby("duplicate")
                .agg(rows=("valid", "size"), pct_coords=("has_coords", "mean"), pct_valid=("valid", "mean"))
            )
            split[["pct_coords", "pct_valid"]] *= 100
            print("\nBy duplicate flag:")
            print(split.round(1).to_string())

    if missing:
        print("\nTypes requested but returned 0 rows (check the spelling):")
        for t in missing:
            print(f"  - {t}")

    print("\n=== OTHER CHECKS ===")
    if "sr_number" in df.columns:
        print(f"Duplicate sr_number values:      {df['sr_number'].duplicated().sum():,}")
    if "status" in df.columns:
        print("\nStatus values:")
        print(df["status"].value_counts().head(10).to_string())
    print(f"\nDate range: {df['created_date'].min()}  to  {df['created_date'].max()}")

    print("\n=== VERDICT (suggested thresholds) ===")
    if pct_valid >= 95:
        print("PASS: 311 is usable as a document source.")
    elif pct_valid >= 80:
        print("PARTIAL: usable, but check which types lose coordinates and consider dropping them.")
    else:
        print("FAIL: too many records can't be placed on the map. Raise this at Gate 2.")


def main() -> None:
    print("Starting Chicago 311 ingestion...")
    print(f"Period: {START} to {END} (exclusive)\nTypes: {len(ROAD_TYPES)}\n")

    all_rows: list[dict] = []
    with requests.Session() as session:
        token = os.environ.get("SOCRATA_APP_TOKEN")  # optional
        if token:
            session.headers["X-App-Token"] = token

        day = START
        while day < END:
            rows = fetch_day(day, session)
            print(f"{day}: {len(rows):,} rows")
            all_rows.extend(rows)
            day += timedelta(days=1)
            time.sleep(0.2)

    if not all_rows:
        raise SystemExit("No rows returned. Check the date range and type names.")

    write_csv(all_rows)
    print(f"\nTotal rows: {len(all_rows):,}")
    print(f"Saved to:   {OUTPUT_FILE}")

    geocoding_report(OUTPUT_FILE)

if __name__ == "__main__":
    main()
