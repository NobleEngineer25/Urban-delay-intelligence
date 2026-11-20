"""Gate 2: validate feed quality on one month and decide go / off-ramp.

Checks each disruption source (311, CDOT permits) on:
    geocoding     share of records placed inside Chicago (after fallback)
    spatial       share of Chicago's 77 community areas with >= 1 record
    temporal      share of days in the month with >= 1 record
    ids           duplicate document IDs
    timestamps    share of records with a parseable start time
and checks weather (a numeric feed, not documents) for day completeness.

A source that FAILs any check is an off-ramp candidate: it should not feed
Pipeline 2 until fixed or replaced. Thresholds are provisional; adjust them
in THRESHOLDS and record the change in docs/decisions.md.

Output: reports/gate2_feed_validation_YYYY_MM.md
Run from the repo root (raw files for the month must exist):
    python pipelines/intelligence/ingest/src/validate_feeds.py --month 2023-01
"""
from __future__ import annotations

import argparse
import calendar
from dataclasses import dataclass
from datetime import date

import geocode_h3 as g
import pandas as pd

REPORTS_DIR = g.PROJECT_ROOT / "reports"
N_COMMUNITY_AREAS = 77

# (pass_at_or_above, partial_at_or_above), in percent.
THRESHOLDS = {
    "geocoding": (95.0, 80.0),
    "spatial": (90.0, 70.0),
    "temporal": (100.0, 90.0),
    "timestamps": (99.0, 95.0),
}
LOW_VOLUME_DAY = 0.25  # warn on days below 25% of the median daily count


@dataclass
class Check:
    source: str
    name: str
    value: str
    status: str  # PASS / PARTIAL / FAIL, or INFO (reported, not graded)
    note: str = ""


def grade(value: float, key: str) -> str:
    pass_at, partial_at = THRESHOLDS[key]
    if value >= pass_at:
        return "PASS"
    if value >= partial_at:
        return "PARTIAL"
    return "FAIL"


def month_bounds(month: str) -> tuple[pd.Timestamp, pd.Timestamp, int]:
    year, mon = (int(x) for x in month.split("-"))
    n_days = calendar.monthrange(year, mon)[1]
    start = pd.Timestamp(date(year, mon, 1))
    return start, start + pd.Timedelta(days=n_days), n_days


def daily_counts(df: pd.DataFrame, start: pd.Timestamp, n_days: int) -> pd.Series:
    """Records starting on each day of the month (311 created, permit start date).
    Counting new records, not active ones, means a feed that stops arriving shows up
    as empty days; long-running permits would otherwise cover every day."""
    days = pd.date_range(start, periods=n_days, freq="D")
    return df["start_time"].dt.floor("D").value_counts().reindex(days, fill_value=0)


def active_counts(df: pd.DataFrame, start: pd.Timestamp, n_days: int) -> pd.Series:
    """Permit segments active on each day (informational only)."""
    days = pd.date_range(start, periods=n_days, freq="D")
    s, e = df["start_time"], df["end_time"]
    return pd.Series(
        [int(((s < day + pd.Timedelta(days=1)) & (e >= day)).sum()) for day in days], index=days
    )


def check_source(df: pd.DataFrame, source: str, month: str) -> tuple[list[Check], list[str]]:
    start, _, n_days = month_bounds(month)
    checks: list[Check] = []
    warnings: list[str] = []
    placed = df["geocode_method"] != "failed"

    # Geocoding
    rate = placed.mean() * 100
    n_block = (df["geocode_method"] == "block_match").sum()
    checks.append(Check(
        source, "Geocoding rate", f"{rate:.2f}% ({placed.sum():,}/{len(df):,})",
        grade(rate, "geocoding"),
        f"{n_block} recovered by block match, {(~placed).sum()} failed",
    ))

    # Spatial coverage. Only 311 carries its own community area; permit areas are
    # borrowed from 311 by H3 cell, so grading them would re-test 311. Report only.
    cas = df.loc[placed, "community_area"].dropna()
    cas = cas[cas.between(1, N_COMMUNITY_AREAS)].astype(int).unique()
    ca_pct = len(cas) / N_COMMUNITY_AREAS * 100
    missing_cas = sorted(set(range(1, N_COMMUNITY_AREAS + 1)) - set(cas))
    own_areas = source == "311"
    checks.append(Check(
        source, "Community areas covered" + ("" if own_areas else " (approx., via 311)"),
        f"{ca_pct:.1f}% ({len(cas)}/{N_COMMUNITY_AREAS})",
        grade(ca_pct, "spatial") if own_areas else "INFO",
        f"{df.loc[placed, 'h3_cell'].nunique():,} H3 cells (res {g.H3_RES})"
        + (f"; missing areas: {missing_cas}" if missing_cas else ""),
    ))

    # Temporal coverage: new records per day. Permits are issued on weekdays, so
    # they are graded on weekdays only.
    counts = daily_counts(df, start, n_days)
    if source == "permits":
        counts = counts[counts.index.day_of_week < 5]
        label, unit = "Weekdays with new records", "weekdays"
    else:
        label, unit = "Days with new records", "days"
    covered = int((counts > 0).sum())
    day_pct = covered / len(counts) * 100
    note = f"daily min/median/max: {counts.min():,}/{int(counts.median()):,}/{counts.max():,}"
    if source == "permits":
        active = active_counts(df, start, n_days)
        note += f"; active segments per day {active.min():,}-{active.max():,}"
    checks.append(Check(
        source, label, f"{day_pct:.1f}% ({covered}/{len(counts)} {unit})", grade(day_pct, "temporal"), note,
    ))
    low = counts[counts < LOW_VOLUME_DAY * counts.median()]
    if len(low):
        warnings.append(
            f"{source}: low-volume {unit} (<{LOW_VOLUME_DAY:.0%} of median): "
            + ", ".join(f"{d:%m-%d} ({c})" for d, c in low.items())
        )

    # IDs
    dup_ids = int(df["doc_id"].duplicated().sum())
    checks.append(Check(source, "Duplicate document IDs", f"{dup_ids:,}", "PASS" if dup_ids == 0 else "PARTIAL"))

    # Timestamps
    ts_pct = df["start_time"].notna().mean() * 100
    checks.append(Check(source, "Parseable start time", f"{ts_pct:.2f}%", grade(ts_pct, "timestamps")))

    # Source-specific notes (informational)
    if source == "311":
        outside = ~df["start_time"].between(start, start + pd.Timedelta(days=n_days), inclusive="left")
        if outside.any():
            warnings.append(f"311: {outside.sum():,} requests created outside {month}.")
    else:
        n_permits = df["group_id"].nunique()
        long = (df["end_time"] - df["start_time"]).dt.days > 365
        warnings.append(
            f"permits: {len(df):,} segments from {n_permits:,} permits; "
            f"{long.sum():,} segments run longer than a year (end dates up to {df['end_time'].max():%Y-%m-%d}), "
            "so treat end dates as permit validity, not actual closure length."
        )
        if "street_closure" in df:
            mix = df["street_closure"].value_counts(normalize=True).mul(100).round(1)
            warnings.append("permits: closure type mix: " + ", ".join(f"{k} {v}%" for k, v in mix.items()))
    return checks, warnings


def check_weather(month: str) -> tuple[list[Check], list[str]]:
    path = g.RAW_DIR / f"chicago_weather_{month.replace('-', '_')}.csv"
    if not path.exists():
        return [Check("weather", "File present", "missing", "FAIL", str(path))], []
    df = pd.read_csv(path)
    _, _, n_days = month_bounds(month)
    checks, warnings = [], []
    for station, grp in df.groupby("STATION"):
        days = grp["DATE"].nunique()
        pct = days / n_days * 100
        checks.append(Check("weather", f"Days present ({station})", f"{days}/{n_days}", grade(pct, "temporal")))
        gaps = {c: int(grp[c].isna().sum()) for c in ("TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND") if c in grp}
        gaps = {c: n for c, n in gaps.items() if n}
        if gaps:
            warnings.append(f"weather {station}: missing values {gaps}")
    return checks, warnings


def decide(checks: list[Check]) -> tuple[str, dict[str, str]]:
    order = ["PASS", "PARTIAL", "FAIL"]
    per_source: dict[str, str] = {}
    for c in checks:
        worst = per_source.get(c.source, "PASS")
        if c.status in order:  # INFO checks never move the decision
            worst = max(worst, c.status, key=order.index)
        per_source[c.source] = worst
    if any(v == "FAIL" for v in per_source.values()):
        overall = "OFF-RAMP"
    elif any(v == "PARTIAL" for v in per_source.values()):
        overall = "GO WITH CAVEATS"
    else:
        overall = "GO"
    return overall, per_source


def render(month: str, rates: pd.DataFrame, checks: list[Check], warnings: list[str],
           overall: str, per_source: dict[str, str]) -> str:
    lines = [
        f"# Gate 2: feed quality validation ({month})",
        "",
        ("Generated by `pipelines/intelligence/ingest/src/validate_feeds.py`. "
        "Thresholds are provisional (see `THRESHOLDS` in the script)."),
        "",
        f"## Decision: **{overall}**",
        "",
        "| Source | Result | Action |",
        "|---|---|---|",
    ]
    actions = {
        "PASS": "Proceed",
        "PARTIAL": "Proceed; address caveats below",
        "FAIL": "Off-ramp: drop or replace this source before Pipeline 2",
    }
    for source, status in per_source.items():
        lines.append(f"| {source} | {status} | {actions[status]} |")

    lines += [
        "",
        f"## Geocoding rate per source (H3 resolution {g.H3_RES})",
        "",
        "| Source | Rows | City lat/lon | Block match | Geocoded | Failed |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for source, r in rates.iterrows():
        lines.append(
            f"| {source} | {int(r['rows']):,} | {r['from_source_pct']:.2f}% | {r['block_match_pct']:.2f}% "
            f"| **{r['geocoded_pct']:.2f}%** | {int(r['failed'])} |"
        )

    lines += ["", "## Checks", "", "| Source | Check | Value | Result | Note |", "|---|---|---|---|---|"]
    for c in checks:
        lines.append(f"| {c.source} | {c.name} | {c.value} | {c.status} | {c.note} |")

    lines += ["", "## Caveats and observations", ""]
    lines += [f"- {w}" for w in warnings] or ["- None."]
    lines += [
        "",
        "## Method",
        "",
        ("- **Geocoding:** city-supplied lat/lon is kept when it falls inside the Chicago bounding box "
        f"(lat {g.LAT_RANGE}, lon {g.LON_RANGE}). Records without it are matched offline to the nearest "
        f"known house number on the same street (within {g.MAX_HOUSE_NUMBER_GAP} numbers, about 400 m), "
        "using every raw 311 and permit file as the address reference."),
        (f"- **H3:** every geocoded record is tagged with its resolution-{g.H3_RES} cell "
        "(output: `data/processed/disruptions_h3_YYYY_MM.parquet`)."),
        ("- **Duplicates:** 311 requests the city flags as duplicates are excluded before every check "
        "and rate above."),
        ("- **Community areas:** 311 records carry one and are graded. Permits borrow the most common "
        "311 community area in the same H3 cell, so their coverage is reported (INFO) but not graded."),
        ("- **Temporal:** counts new records per day (311 created date, permit start date), so a feed "
        "that stops arriving shows empty days. Permits are graded on weekdays only."),
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", default="2023-01", help="YYYY-MM (default 2023-01)")
    args = parser.parse_args()

    df = g.run(args.month)
    city_dups = df["is_duplicate"].fillna(False).astype(bool)
    n_311 = int((df["source"] == "311").sum())
    df = df[~city_dups]
    rates = g.geocoding_rates(df)

    checks: list[Check] = []
    warnings: list[str] = [
        (f"311: excluded {int(city_dups.sum()):,} of {n_311:,} requests "
        f"({city_dups.sum() / n_311:.1%}) flagged duplicate by the city; all figures use the rest.")
    ]
    for source in ("311", "permits"):
        c, w = check_source(df[df["source"] == source], source, args.month)
        checks += c
        warnings += w
    c, w = check_weather(args.month)
    checks += c
    warnings += w

    overall, per_source = decide(checks)
    report = render(args.month, rates, checks, warnings, overall, per_source)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / f"gate2_feed_validation_{args.month.replace('-', '_')}.md"
    out.write_text(report, encoding="utf-8")
    print(report)
    print(f"Saved report to {out}")


if __name__ == "__main__":
    main()
