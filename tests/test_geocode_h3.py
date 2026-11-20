import sys
from pathlib import Path

import h3
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines/intelligence/ingest/src"))

import geocode_h3 as g
import validate_feeds as v


def make_records(rows):
    base = {
        "source": "311", "doc_id": None, "group_id": None, "street_key": "W|OAKDALE|AVE",
        "house_number": None, "src_lat": None, "src_lon": None, "community_area": None,
    }
    df = pd.DataFrame([{**base, **r} for r in rows])
    for col in ("house_number", "src_lat", "src_lon", "community_area"):
        df[col] = df[col].astype("float64")
    return df


def test_source_coordinates_kept_and_out_of_city_rejected():
    df = make_records([
        {"doc_id": "a", "src_lat": 41.93, "src_lon": -87.75},
        {"doc_id": "b", "src_lat": 0.0, "src_lon": 0.0},
    ])
    out = g.geocode(df, g.build_gazetteer([df]))
    assert out["geocode_method"].tolist() == ["source", "failed"]


def test_block_match_uses_nearest_house_number_within_gap():
    df = make_records([
        {"doc_id": "known1", "house_number": 5100, "src_lat": 41.930, "src_lon": -87.750},
        {"doc_id": "known2", "house_number": 5400, "src_lat": 41.931, "src_lon": -87.760},
        {"doc_id": "near", "house_number": 5149},
        {"doc_id": "far", "house_number": 9000},
    ])
    out = g.geocode(df, g.build_gazetteer([df])).set_index("doc_id")
    assert out.loc["near", "geocode_method"] == "block_match"
    assert (out.loc["near", "lat"], out.loc["near", "lon"]) == (41.930, -87.750)
    assert out.loc["far", "geocode_method"] == "failed"


def test_tag_h3_and_rates():
    df = make_records([
        {"doc_id": "a", "src_lat": 41.88, "src_lon": -87.63},
        {"doc_id": "b"},
    ])
    out = g.tag_h3(g.geocode(df, g.build_gazetteer([df])))
    assert out.loc[0, "h3_cell"] == h3.latlng_to_cell(41.88, -87.63, g.H3_RES)
    assert pd.isna(out.loc[1, "h3_cell"])
    rates = g.geocoding_rates(out)
    assert rates.loc["311", "geocoded_pct"] == 50.0


def test_grade_and_decision():
    assert v.grade(99.0, "geocoding") == "PASS"
    assert v.grade(85.0, "geocoding") == "PARTIAL"
    assert v.grade(50.0, "geocoding") == "FAIL"
    checks = [v.Check("311", "x", "", "PASS"), v.Check("permits", "x", "", "FAIL")]
    overall, per_source = v.decide(checks)
    assert overall == "OFF-RAMP"
    assert per_source == {"311": "PASS", "permits": "FAIL"}


def test_info_checks_do_not_affect_decision():
    checks = [v.Check("permits", "x", "", "PASS"), v.Check("permits", "areas", "", "INFO")]
    assert v.decide(checks) == ("GO", {"permits": "PASS"})


def test_daily_counts_counts_new_records_not_active_ones():
    start, _, n_days = v.month_bounds("2023-01")
    # A permit that started in 2017 and is still active should not fill any January day.
    df = pd.DataFrame({
        "start_time": pd.to_datetime(["2017-07-17", "2023-01-03", "2023-01-03"]),
        "end_time": pd.to_datetime(["2099-01-01", "2023-01-10", "2023-01-10"]),
    })
    counts = v.daily_counts(df, start, n_days)
    assert counts.sum() == 2
    assert counts[pd.Timestamp("2023-01-03")] == 2
    assert v.active_counts(df, start, n_days)[pd.Timestamp("2023-01-20")] == 1
