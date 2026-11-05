"""Download CDOT street closure permits active during one month, then check them.

Raw output: data/raw/chicago_permits_YYYY_MM.csv
Run from the repo root:
    python pipelines/intelligence/ingest/src/ingest_permits.py
"""
import csv
import os
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[4]
assert (PROJECT_ROOT / ".git").exists(), f"Wrong project root: {PROJECT_ROOT}"

BASE_URL = "https://data.cityofchicago.org/resource/jdis-5sry.json"

# Same month as the taxi and 311 data. END is exclusive.
START = date(2023, 1, 1)
END = date(2023, 2, 1)

OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / f"chicago_permits_{START:%Y_%m}.csv"

START_COL = "applicationstartdate"
END_COL = "applicationenddate"
ORDER_COL = "uniquekey"  # a unique column keeps paging stable

PAGE_SIZE = 50_000
LAT_RANGE = (41.6, 42.1)
LON_RANGE = (-88.0, -87.5)


def fetch_active_permits(session: requests.Session) -> list[dict]:
    """A permit is active in the window if it starts before the window ends
    AND ends on or after the window starts. This keeps closures that began
    before the month but were still running during it."""
    where = (
        f"{START_COL} < '{END.isoformat()}T00:00:00' "
        f"AND {END_COL} >= '{START.isoformat()}T00:00:00'"
    )
    rows: list[dict] = []
    offset = 0
    while True:
        params = {"$where": where, "$order": ORDER_COL, "$limit": PAGE_SIZE, "$offset": offset}
        response = session.get(BASE_URL, params=params, timeout=120)
        response.raise_for_status()
        page = response.json()
        print(f"Offset {offset:,}: {len(page):,} rows")
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.2)
    return rows


def write_csv(rows: list[dict]) -> None:
    """Socrata omits null fields, so build columns from ALL rows."""
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


def report(path: Path) -> None:
    df = pd.read_csv(path, low_memory=False)

    for col in (START_COL, END_COL, "latitude", "longitude"):
        if col not in df.columns:
            raise SystemExit(f"Column '{col}' not found.\nColumns are: {list(df.columns)}")

    start = pd.to_datetime(df[START_COL], errors="coerce")
    end = pd.to_datetime(df[END_COL], errors="coerce")
    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")
    has_coords = lat.notna() & lon.notna()
    in_chicago = has_coords & lat.between(*LAT_RANGE) & lon.between(*LON_RANGE)
    duration_days = (end - start).dt.total_seconds() / 86400

    total = len(df)
    print("\n=== VOLUME ===")
    print(f"Rows active during the window:     {total:,}")
    if "applicationnumber" in df.columns:
        n_permits = df["applicationnumber"].nunique()
        print(f"Distinct permits:                  {n_permits:,}")
        print(f"Rows per permit (avg):             {total / max(n_permits, 1):.2f}")
    began_before = (start < pd.Timestamp(START)).sum()
    print(f"Started before the window:         {began_before:,}")

    print("\n=== GEOCODING ===")
    print(f"Have latitude + longitude:         {has_coords.sum():,}  ({has_coords.mean() * 100:.1f}%)")
    print(f"Coordinates inside Chicago box:    {in_chicago.sum():,}  ({in_chicago.mean() * 100:.1f}%)")

    print("\n=== DURATION (days) ===")
    bad_dates = start.isna() | end.isna() | (end < start)
    print(f"Unparseable or end-before-start:   {bad_dates.sum():,}")
    valid = duration_days[~bad_dates]
    print(valid.quantile([0.5, 0.9, 0.99]).round(1).rename("days").to_string())
    print(f"Longest:                           {valid.max():.0f} days")
    print(f"Longer than 90 days:               {(valid > 90).sum():,}")

    for col in ("worktypedescription", "worktype", "applicationstatus"):
        if col in df.columns:
            print(f"\n=== {col} ===")
            print(df[col].value_counts().head(15).to_string())

    pct_valid = in_chicago.mean() * 100
    print("\n=== VERDICT (suggested thresholds) ===")
    if pct_valid >= 95:
        print("PASS: permits are usable as a document source.")
    elif pct_valid >= 80:
        print("PARTIAL: usable, but check which work types lose coordinates.")
    else:
        print("FAIL: too many permits can't be placed on the map. Raise this at Gate 2.")


def main() -> None:
    print("Starting CDOT street closure permit ingestion...")
    print(f"Permits active between {START} and {END} (exclusive)\n")

    with requests.Session() as session:
        token = os.environ.get("SOCRATA_APP_TOKEN")  # optional
        if token:
            session.headers["X-App-Token"] = token
        rows = fetch_active_permits(session)

    if not rows:
        raise SystemExit("No rows returned. Check the date fields and the window.")

    write_csv(rows)
    print(f"\nSaved {len(rows):,} rows to {OUTPUT_FILE}")
    report(OUTPUT_FILE)


if __name__ == "__main__":
    main()