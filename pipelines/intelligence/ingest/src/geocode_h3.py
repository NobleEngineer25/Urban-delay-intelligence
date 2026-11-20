"""Geocode and H3-tag one month of disruption data (311 requests + CDOT permits).

Each record ends up with:
    lat, lon         final coordinates (or empty if it could not be placed)
    geocode_method   "source"       city-supplied lat/lon, inside Chicago
                     "block_match"  rebuilt from the street address (see below)
                     "failed"       no usable coordinates
    h3_cell          H3 cell at H3_RES (empty if failed)

Fallback geocoding is offline: a gazetteer of every street address that does
have city coordinates (all raw 311 and permit files). A record without
coordinates is placed at the nearest known house number on the same street,
if one exists within MAX_HOUSE_NUMBER_GAP. In Chicago's grid, 800 house
numbers is one mile, so 200 numbers is about 400 m: well inside one
resolution-8 cell's diameter.

Output: data/processed/disruptions_h3_YYYY_MM.parquet
Run from the repo root:
    python pipelines/intelligence/ingest/src/geocode_h3.py --month 2023-01
"""
from __future__ import annotations

import argparse
from pathlib import Path

import h3
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Resolution 8: ~0.74 km² cells, ~460 m edge. Must match the H3 resolution
# Pipeline 1 uses for taxi trips, so disruptions join to the same cells.
H3_RES = 8

# Rough Chicago bounding box, to catch bad geocodes like (0, 0).
LAT_RANGE = (41.6, 42.1)
LON_RANGE = (-88.0, -87.5)

MAX_HOUSE_NUMBER_GAP = 200

OUTPUT_COLUMNS = [
    "source", "doc_id", "group_id", "event_type", "status",
    "start_time", "end_time", "street_address",
    "lat", "lon", "geocode_method", "h3_cell", "community_area",
]


# ---------------------------------------------------------------- loading

def _clean(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.upper().replace("", pd.NA)


def _street_key(direction, name, suffix) -> pd.Series:
    """Normalised street identity, e.g. 'W|OAKDALE|AVE'."""
    return _clean(direction).fillna("") + "|" + _clean(name).fillna("") + "|" + _clean(suffix).fillna("")


def load_311(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False, dtype=str)
    return pd.DataFrame({
        "source": "311",
        "doc_id": df["sr_number"],
        "group_id": df["sr_number"],
        "event_type": df["sr_type"],
        "status": df.get("status"),
        "start_time": pd.to_datetime(df["created_date"], errors="coerce"),
        "end_time": pd.to_datetime(df.get("closed_date"), errors="coerce"),
        "street_address": df.get("street_address"),
        "src_lat": pd.to_numeric(df["latitude"], errors="coerce").astype("float64"),
        "src_lon": pd.to_numeric(df["longitude"], errors="coerce").astype("float64"),
        "street_key": _street_key(df["street_direction"], df["street_name"], df["street_type"]),
        "house_number": pd.to_numeric(df["street_number"], errors="coerce").astype("float64"),
        "community_area": pd.to_numeric(df.get("community_area"), errors="coerce").astype("float64"),
        "is_duplicate": df.get("duplicate", pd.Series("False", index=df.index)).str.lower().eq("true"),
    })


def load_permits(path: Path) -> pd.DataFrame:
    """One row per permit segment. A permit (applicationnumber) can have several."""
    df = pd.read_csv(path, low_memory=False, dtype=str)
    num_from = pd.to_numeric(df["streetnumberfrom"], errors="coerce").astype("float64")
    num_to = pd.to_numeric(df["streetnumberto"], errors="coerce").astype("float64")
    address = (
        df["streetnumberfrom"].fillna("") + "-" + df["streetnumberto"].fillna("") + " "
        + df["direction"].fillna("") + " " + df["streetname"].fillna("") + " " + df["suffix"].fillna("")
    ).str.strip()
    return pd.DataFrame({
        "source": "permits",
        "doc_id": df["uniquekey"],
        "group_id": df["applicationnumber"],
        "event_type": df["worktypedescription"],
        "status": df.get("applicationstatus"),
        "start_time": pd.to_datetime(df["applicationstartdate"], errors="coerce"),
        "end_time": pd.to_datetime(df["applicationenddate"], errors="coerce"),
        "street_address": address,
        "src_lat": pd.to_numeric(df["latitude"], errors="coerce").astype("float64"),
        "src_lon": pd.to_numeric(df["longitude"], errors="coerce").astype("float64"),
        "street_key": _street_key(df["direction"], df["streetname"], df["suffix"]),
        "house_number": (num_from + num_to.fillna(num_from)) / 2,  # segment midpoint
        "community_area": np.nan,  # not in the feed; filled from 311 by H3 cell
        "is_duplicate": False,
        "street_closure": df.get("streetclosure"),
    })


# -------------------------------------------------------------- geocoding

def in_chicago(lat: pd.Series, lon: pd.Series) -> pd.Series:
    return lat.between(*LAT_RANGE) & lon.between(*LON_RANGE)


def build_gazetteer(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Known (street, house number) -> coordinates, from records the city geocoded."""
    known = pd.concat(frames, ignore_index=True)
    ok = (
        in_chicago(known["src_lat"], known["src_lon"])
        & known["house_number"].notna()
        & known["street_key"].str.replace("|", "", regex=False).str.len().gt(0)
    )
    return (
        known.loc[ok]
        .groupby(["street_key", "house_number"], as_index=False)[["src_lat", "src_lon"]]
        .median()
        .rename(columns={"src_lat": "lat", "src_lon": "lon"})
    )


def block_match(df: pd.DataFrame, gazetteer: pd.DataFrame) -> pd.DataFrame:
    """Nearest known house number on the same street, within MAX_HOUSE_NUMBER_GAP.
    Returns lat/lon aligned to df's index (NaN where no match)."""
    out = pd.DataFrame({"lat": np.nan, "lon": np.nan}, index=df.index)
    by_street = {key: grp for key, grp in gazetteer.groupby("street_key")}
    for idx, row in df.iterrows():
        candidates = by_street.get(row["street_key"])
        if candidates is None or pd.isna(row["house_number"]):
            continue
        gap = (candidates["house_number"] - row["house_number"]).abs()
        best = gap.idxmin()
        if gap[best] <= MAX_HOUSE_NUMBER_GAP:
            out.loc[idx, ["lat", "lon"]] = candidates.loc[best, ["lat", "lon"]].to_numpy()
    return out


def geocode(df: pd.DataFrame, gazetteer: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    has_source = in_chicago(df["src_lat"], df["src_lon"])
    df["lat"] = df["src_lat"].where(has_source)
    df["lon"] = df["src_lon"].where(has_source)
    df["geocode_method"] = np.where(has_source, "source", "failed")

    missing = ~has_source
    if missing.any():
        matched = block_match(df.loc[missing], gazetteer)
        hit = matched["lat"].notna()
        hit_idx = matched.index[hit]
        df.loc[hit_idx, ["lat", "lon"]] = matched.loc[hit_idx, ["lat", "lon"]]
        df.loc[hit_idx, "geocode_method"] = "block_match"
    return df


def tag_h3(df: pd.DataFrame, res: int = H3_RES) -> pd.DataFrame:
    df = df.copy()
    placed = df["lat"].notna() & df["lon"].notna()
    df["h3_cell"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df.loc[placed, "h3_cell"] = [
        h3.latlng_to_cell(lat, lon, res) for lat, lon in zip(df.loc[placed, "lat"], df.loc[placed, "lon"])
    ]
    return df


def fill_community_area(df: pd.DataFrame) -> pd.DataFrame:
    """Permits have no community area. Borrow the most common 311 community
    area in the same H3 cell (approximate: a cell can straddle two areas)."""
    df = df.copy()
    ref = df.loc[(df["source"] == "311") & df["community_area"].notna() & df["h3_cell"].notna()]
    cell_to_ca = ref.groupby("h3_cell")["community_area"].agg(lambda s: s.mode().iloc[0])
    need = df["community_area"].isna() & df["h3_cell"].notna()
    df.loc[need, "community_area"] = df.loc[need, "h3_cell"].map(cell_to_ca).astype("float64")
    return df


# --------------------------------------------------------------- pipeline

def raw_paths(month: str) -> dict[str, Path]:
    tag = month.replace("-", "_")
    return {
        "311": RAW_DIR / f"chicago_311_{tag}.csv",
        "permits": RAW_DIR / f"chicago_permits_{tag}.csv",
    }


def run(month: str) -> pd.DataFrame:
    paths = raw_paths(month)
    for name, path in paths.items():
        if not path.exists():
            raise SystemExit(f"Missing {name} file: {path}. Run the ingest script for {month} first.")

    month_frames = [load_311(paths["311"]), load_permits(paths["permits"])]

    # Gazetteer uses every raw file, not just this month: addresses don't move.
    all_frames = [load_311(p) for p in sorted(RAW_DIR.glob("chicago_311_*.csv"))]
    all_frames += [load_permits(p) for p in sorted(RAW_DIR.glob("chicago_permits_*.csv"))]
    gazetteer = build_gazetteer(all_frames)

    df = pd.concat(month_frames, ignore_index=True)
    df = geocode(df, gazetteer)
    df = tag_h3(df)
    df = fill_community_area(df)
    return df


def geocoding_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Per-source geocoding rate, split by method."""
    counts = pd.crosstab(df["source"], df["geocode_method"])
    for col in ("source", "block_match", "failed"):
        if col not in counts:
            counts[col] = 0
    counts = counts[["source", "block_match", "failed"]]
    rates = pd.DataFrame({
        "rows": counts.sum(axis=1),
        "from_source_pct": counts["source"] / counts.sum(axis=1) * 100,
        "block_match_pct": counts["block_match"] / counts.sum(axis=1) * 100,
        "geocoded_pct": (counts["source"] + counts["block_match"]) / counts.sum(axis=1) * 100,
        "failed": counts["failed"],
    })
    rates.index.name = "source"
    return rates


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", default="2023-01", help="YYYY-MM (default 2023-01)")
    args = parser.parse_args()

    df = run(args.month)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / f"disruptions_h3_{args.month.replace('-', '_')}.parquet"
    extra = [c for c in ("is_duplicate", "street_closure") if c in df.columns]
    df[OUTPUT_COLUMNS + extra].to_parquet(out, index=False)

    print(f"=== GEOCODING RATE BY SOURCE ({args.month}, H3 res {H3_RES}) ===")
    print(geocoding_rates(df).round(2).to_string())
    print(f"\nDistinct H3 cells: {df['h3_cell'].nunique():,}")
    print(f"Saved {len(df):,} rows to {out}")


if __name__ == "__main__":
    main()
