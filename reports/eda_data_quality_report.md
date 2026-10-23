# EDA and Data Quality Report

## 1. Dataset overview

- **Source:** `chicago_taxi_2023_01.csv`
- **Rows:** 414,478
- **Columns:** 26

### Schema

| Column | Data type |
|---|---|
| `trip_id` | `string` |
| `taxi_id` | `string` |
| `trip_start_timestamp` | `timestamp` |
| `trip_end_timestamp` | `timestamp` |
| `trip_seconds` | `int` |
| `trip_miles` | `double` |
| `pickup_community_area` | `int` |
| `dropoff_community_area` | `int` |
| `fare` | `double` |
| `tips` | `double` |
| `tolls` | `double` |
| `extras` | `double` |
| `trip_total` | `double` |
| `payment_type` | `string` |
| `company` | `string` |
| `pickup_centroid_latitude` | `double` |
| `pickup_centroid_longitude` | `double` |
| `pickup_centroid_location` | `string` |
| `dropoff_centroid_latitude` | `double` |
| `dropoff_centroid_longitude` | `double` |
| `dropoff_centroid_location` | `string` |
| `:id` | `string` |
| `:version` | `string` |
| `:created_at` | `timestamp` |
| `:updated_at` | `timestamp` |
| `:@computed_region_vrxf_vc4k` | `int` |

## 2. Time coverage

- **First trip start:** 2023-01-01 00:00:00
- **Last trip start:** 2023-01-31 23:45:00

### Trips by hour of day

| Hour | Trips |
|---:|---:|
| 0 | 7,470 |
| 1 | 5,003 |
| 2 | 3,303 |
| 3 | 2,399 |
| 4 | 2,511 |
| 5 | 3,777 |
| 6 | 7,144 |
| 7 | 14,060 |
| 8 | 21,295 |
| 9 | 23,076 |
| 10 | 22,525 |
| 11 | 23,926 |
| 12 | 26,239 |
| 13 | 26,879 |
| 14 | 26,650 |
| 15 | 27,902 |
| 16 | 29,392 |
| 17 | 31,000 |
| 18 | 28,762 |
| 19 | 25,943 |
| 20 | 19,430 |
| 21 | 15,247 |
| 22 | 11,669 |
| 23 | 8,876 |

### Trips by date

| Date | Trips |
|---|---:|
| 2023-01-01 | 11,051 |
| 2023-01-02 | 9,522 |
| 2023-01-03 | 13,192 |
| 2023-01-04 | 13,040 |
| 2023-01-05 | 13,566 |
| 2023-01-06 | 13,303 |
| 2023-01-07 | 9,993 |
| 2023-01-08 | 9,064 |
| 2023-01-09 | 13,331 |
| 2023-01-10 | 14,255 |
| 2023-01-11 | 15,054 |
| 2023-01-12 | 16,530 |
| 2023-01-13 | 15,987 |
| 2023-01-14 | 11,183 |
| 2023-01-15 | 9,674 |
| 2023-01-16 | 11,946 |
| 2023-01-17 | 15,838 |
| 2023-01-18 | 15,734 |
| 2023-01-19 | 16,655 |
| 2023-01-20 | 15,238 |
| 2023-01-21 | 10,370 |
| 2023-01-22 | 9,215 |
| 2023-01-23 | 14,312 |
| 2023-01-24 | 15,336 |
| 2023-01-25 | 15,860 |
| 2023-01-26 | 17,816 |
| 2023-01-27 | 16,144 |
| 2023-01-28 | 10,802 |
| 2023-01-29 | 9,534 |
| 2023-01-30 | 14,565 |
| 2023-01-31 | 16,368 |

## 3. Missing values

| Column | Missing/null/NaN | Percentage |
|---|---:|---:|
| `trip_id` | 0 | 0.00% |
| `taxi_id` | 0 | 0.00% |
| `trip_start_timestamp` | 0 | 0.00% |
| `trip_end_timestamp` | 17 | 0.00% |
| `trip_seconds` | 100 | 0.02% |
| `trip_miles` | 0 | 0.00% |
| `pickup_community_area` | 25,654 | 6.19% |
| `dropoff_community_area` | 40,034 | 9.66% |
| `fare` | 405 | 0.10% |
| `tips` | 405 | 0.10% |
| `tolls` | 405 | 0.10% |
| `extras` | 405 | 0.10% |
| `trip_total` | 405 | 0.10% |
| `payment_type` | 0 | 0.00% |
| `company` | 0 | 0.00% |
| `pickup_centroid_latitude` | 25,593 | 6.17% |
| `pickup_centroid_longitude` | 25,593 | 6.17% |
| `pickup_centroid_location` | 25,593 | 6.17% |
| `dropoff_centroid_latitude` | 38,458 | 9.28% |
| `dropoff_centroid_longitude` | 38,458 | 9.28% |
| `dropoff_centroid_location` | 38,458 | 9.28% |
| `:id` | 0 | 0.00% |
| `:version` | 0 | 0.00% |
| `:created_at` | 0 | 0.00% |
| `:updated_at` | 0 | 0.00% |
| `:@computed_region_vrxf_vc4k` | 25,593 | 6.17% |

## 4. Duplicate records

- **Exact duplicate rows:** 0
- **Distinct non-null trip IDs:** 414,478
- **Missing trip IDs:** 0

## 5. Numeric distributions

| Column | Min | Mean | Median | P95 | Max |
|---|---:|---:|---:|---:|---:|
| `trip_seconds` | 0.000 | 1,099.599 | 832.000 | 2,599.000 | 86,340.000 |
| `trip_miles` | 0.000 | 6.110 | 2.610 | 18.000 | 899.500 |
| `pickup_community_area` | 1.000 | 33.466 | 32.000 | 76.000 | 77.000 |
| `dropoff_community_area` | 1.000 | 25.151 | 28.000 | 76.000 | 77.000 |
| `fare` | 0.000 | 20.705 | 13.800 | 46.250 | 1,111.110 |
| `tips` | 0.000 | 2.449 | 0.000 | 10.000 | 133.500 |
| `tolls` | 0.000 | 0.030 | 0.000 | 0.000 | 4,444.440 |
| `extras` | 0.000 | 1.890 | 0.000 | 6.000 | 8,888.880 |
| `trip_total` | 0.000 | 25.201 | 16.500 | 60.950 | 8,902.630 |
| `pickup_centroid_latitude` | 41.652 | 41.899 | 41.895 | 41.980 | 42.021 |
| `pickup_centroid_longitude` | -87.914 | -87.692 | -87.643 | -87.616 | -87.531 |
| `dropoff_centroid_latitude` | 41.660 | 41.892 | 41.893 | 41.984 | 42.016 |
| `dropoff_centroid_longitude` | -87.914 | -87.658 | -87.633 | -87.616 | -87.535 |
| `:@computed_region_vrxf_vc4k` | 1.000 | 43.861 | 38.000 | 75.000 | 77.000 |

## 6. Data quality checks

| Check | Result |
|---|---:|
| Missing trip_seconds | 100 rows |
| Zero trip_seconds | 7,924 rows |
| Negative trip_seconds | 0 rows |
| Missing trip_miles | 0 rows |
| Zero trip_miles | 44,519 rows |
| Negative trip_miles | 0 rows |
| Missing trip end timestamp | 17 rows |
| Trip end before trip start | 3 rows |

## 7. Categorical distributions

### payment_type

| Value | Count | Percentage |
|---|---:|---:|
| Credit Card | 141,229 | 34.07% |
| Cash | 126,713 | 30.57% |
| Mobile | 62,799 | 15.15% |
| Prcard | 57,135 | 13.78% |
| Unknown | 26,273 | 6.34% |
| No Charge | 240 | 0.06% |
| Dispute | 89 | 0.02% |

### company

| Value | Count | Percentage |
|---|---:|---:|
| Flash Cab | 89,482 | 21.59% |
| Taxi Affiliation Services | 82,370 | 19.87% |
| Sun Taxi | 43,075 | 10.39% |
| City Service | 40,308 | 9.73% |
| Taxicab Insurance Agency Llc | 31,192 | 7.53% |
| Chicago Independents | 22,879 | 5.52% |
| 5 Star Taxi | 21,052 | 5.08% |
| Globe Taxi | 17,309 | 4.18% |
| Medallion Leasin | 13,559 | 3.27% |
| Taxicab Insurance Agency, LLC | 11,588 | 2.80% |
| Blue Ribbon Taxi Association | 10,508 | 2.54% |
| Star North Taxi Management Llc | 8,273 | 2.00% |
| Choice Taxi Association | 7,157 | 1.73% |
| Top Cab Affiliation | 6,659 | 1.61% |
| U Taxicab | 2,956 | 0.71% |
| Koam Taxi Association | 1,467 | 0.35% |
| Patriot Taxi Dba Peace Taxi Associat | 1,355 | 0.33% |
| 312 Medallion Management Corp | 593 | 0.14% |
| Chicago Taxicab | 577 | 0.14% |
| Setare Inc | 529 | 0.13% |
| Top Cab | 431 | 0.10% |
| Metro Jet Taxi A. | 340 | 0.08% |
| 3556 - 36214 RC Andrews Cab | 194 | 0.05% |
| 6574 - Babylon Express Inc. | 132 | 0.03% |
| Leonard Cab Co | 105 | 0.03% |
| Taxi Affiliation Services Llc - Yell | 100 | 0.02% |
| Petani Cab Corp | 73 | 0.02% |
| 4787 - 56058 Reny Cab Co | 53 | 0.01% |
| 4053 - 40193 Adwar H. Nikola | 49 | 0.01% |
| 4623 - 27290 Jay Kim | 48 | 0.01% |
| 2733 - 74600 Benny Jona | 34 | 0.01% |
| 5062 - 34841 Sam Mestas | 31 | 0.01% |


## 8. Extreme-value investigation

These are the highest observed values, not automatically confirmed errors.

### Highest values for `trip_seconds`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| f7b75e13953a4cec8e339817a4da09f54fe7150a | 2023-01-29 13:00:00 | 2023-01-30 13:00:00 | 86340 | 0.0 | 29.0 | 0.0 | 0.0 | 36.88 |
| 4abb7cfb774c03e2d4cfb45524ef8de8fd65e81e | 2023-01-31 10:45:00 | 2023-02-01 10:45:00 | 86340 | 0.0 | 38.0 | 0.0 | 0.0 | 46.2 |
| ea138d778cb63034cabcbb81031b7946a513e08b | 2023-01-31 10:45:00 | 2023-02-01 10:45:00 | 86340 | 0.0 | 38.0 | 0.0 | 0.0 | 46.2 |
| 1c43d2370a4103049e2a47bb2624c79bc68504c6 | 2023-01-27 08:45:00 | 2023-01-28 08:45:00 | 86164 | 0.0 | 3.25 | 0.0 | 0.0 | 3.25 |
| 507555829e3b56b2b8dfadb22970a2dfdb2233c0 | 2023-01-28 12:45:00 | 2023-01-29 12:45:00 | 86160 | 0.0 | 57.0 | 0.0 | 0.0 | 62.5 |

### Highest values for `trip_miles`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| 82554de7bb093dd8a993701b1de8a32f49b624d6 | 2023-01-02 14:30:00 | 2023-01-02 15:00:00 | 2280 | 899.5 | 43.75 | 0.0 | 0.0 | 43.75 |
| e9535cb38d527b0a130a1263c7a862d3d90f492e | 2023-01-31 21:30:00 | 2023-01-31 21:30:00 | 0 | 802.6 | 45.0 | 0.0 | 5.0 | 50.0 |
| ea299070725cd8c3a91176e138ee57585717b27b | 2023-01-02 21:00:00 | 2023-01-02 21:15:00 | 1260 | 750.4 | 36.5 | 0.0 | 0.0 | 36.5 |
| d6111a41b1c41ce2968173e28f5367840024bc35 | 2023-01-26 22:45:00 | 2023-01-27 00:00:00 | 4980 | 735.5 | 44.75 | 0.0 | 4.0 | 59.25 |
| 815e67a836fc5a6e3b6d154c78bb81a5f1725680 | 2023-01-05 19:00:00 | 2023-01-05 19:00:00 | 0 | 711.9 | 44.25 | 0.0 | 4.0 | 48.25 |

### Highest values for `fare`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| dcd514a57b7756c950cda7c4c15783f16204b8ff | 2023-01-16 17:30:00 | 2023-01-16 17:30:00 | 120 | 0.0 | 1111.11 | 0.0 | 0.0 | 1111.11 |
| b1277c82fdbe1f4bbe498086ca338c0ee8b59e8d | 2023-01-25 11:45:00 | 2023-01-25 11:45:00 | 165 | 0.0 | 1061.24 | 0.0 | 0.5 | 1061.74 |
| 97ebfaf1be5dcf03d7e9d614509c41e6ecfea7e7 | 2023-01-13 15:15:00 | 2023-01-13 15:15:00 | 0 | 0.0 | 999.99 | 0.0 | 0.0 | 999.99 |
| 8e7ae6d46d3c6a073aba2a2c441791c4c7b5cdc4 | 2023-01-12 07:30:00 | 2023-01-12 07:30:00 | 0 | 19.3 | 837.8 | 0.0 | 7.0 | 844.8 |
| afac9e0c982a04cc05603110aa4a66a69d0d8b1a | 2023-01-17 10:30:00 | 2023-01-18 01:15:00 | 53214 | 229.85 | 712.5 | 0.0 | 0.0 | 712.5 |

### Highest values for `tolls`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| 2d9c88012d3dfc29cf0f9e53cab4c65613a2bb65 | 2023-01-31 11:15:00 | 2023-01-31 11:30:00 | 1020 | 0.2 | 13.75 | 4444.44 | 4444.44 | 8902.63 |
| d601257bb6c3fe8fc6b08586535a72eb3bf1ddcc | 2023-01-19 11:45:00 | 2023-01-19 11:45:00 | 300 | 0.0 | 6.5 | 3333.33 | 3334.33 | 6674.16 |
| d7b68d8a6c76af43412175eb11a9ec8b5b946a9f | 2023-01-30 20:30:00 | 2023-01-30 21:15:00 | 2737 | 42.29 | 98.75 | 54.0 | 10.0 | 163.25 |
| eebf403ad91211ebe662cd6028ade0fe177658b9 | 2023-01-15 18:30:00 | 2023-01-15 18:30:00 | 87 | 0.16 | 3.5 | 54.0 | 0.5 | 58.0 |
| c0008d27249d1195972d0199da56e090fa8d804c | 2023-01-06 19:45:00 | 2023-01-06 19:45:00 | 35 | 0.0 | 3.25 | 52.0 | 0.0 | 55.75 |

### Highest values for `extras`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| 3c8343165cd6271f8d2a6cae9b8a5c46746e75d6 | 2023-01-05 14:45:00 | 2023-01-05 15:00:00 | 480 | 0.0 | 6.75 | 0.88 | 8888.88 | 8896.51 |
| 2d9c88012d3dfc29cf0f9e53cab4c65613a2bb65 | 2023-01-31 11:15:00 | 2023-01-31 11:30:00 | 1020 | 0.2 | 13.75 | 4444.44 | 4444.44 | 8902.63 |
| 2bcedd8086ed168f7f02d916f2ba55a09c52b235 | 2023-01-11 22:00:00 | 2023-01-11 22:30:00 | 1080 | 0.5 | 23.0 | 0.0 | 4431.44 | 4454.44 |
| d601257bb6c3fe8fc6b08586535a72eb3bf1ddcc | 2023-01-19 11:45:00 | 2023-01-19 11:45:00 | 300 | 0.0 | 6.5 | 3333.33 | 3334.33 | 6674.16 |
| 2dceed72db0fdcfd45c4b72da2e63c41fb539b4e | 2023-01-12 12:15:00 | 2023-01-12 12:15:00 | 0 | 0.0 | 3.25 | 1.11 | 1111.11 | 1115.47 |

### Highest values for `trip_total`

| trip_id | trip_start_timestamp | trip_end_timestamp | trip_seconds | trip_miles | fare | tolls | extras | trip_total |
|---|---|---|---|---|---|---|---|---|
| 2d9c88012d3dfc29cf0f9e53cab4c65613a2bb65 | 2023-01-31 11:15:00 | 2023-01-31 11:30:00 | 1020 | 0.2 | 13.75 | 4444.44 | 4444.44 | 8902.63 |
| 3c8343165cd6271f8d2a6cae9b8a5c46746e75d6 | 2023-01-05 14:45:00 | 2023-01-05 15:00:00 | 480 | 0.0 | 6.75 | 0.88 | 8888.88 | 8896.51 |
| d601257bb6c3fe8fc6b08586535a72eb3bf1ddcc | 2023-01-19 11:45:00 | 2023-01-19 11:45:00 | 300 | 0.0 | 6.5 | 3333.33 | 3334.33 | 6674.16 |
| 2bcedd8086ed168f7f02d916f2ba55a09c52b235 | 2023-01-11 22:00:00 | 2023-01-11 22:30:00 | 1080 | 0.5 | 23.0 | 0.0 | 4431.44 | 4454.44 |
| 2dceed72db0fdcfd45c4b72da2e63c41fb539b4e | 2023-01-12 12:15:00 | 2023-01-12 12:15:00 | 0 | 0.0 | 3.25 | 1.11 | 1111.11 | 1115.47 |


## 9. Interpretation and limitations

- Missing geographic fields may limit spatial analysis.
- Zero or negative durations should be investigated separately.
- Extreme distance and fare values need domain validation.
- Exact duplicate checks do not replace business-key checks.
- No source rows are changed or deleted by this script.
- These findings describe the raw CSV before Bronze ingestion.
- Follow-up checks should compare raw, Bronze, Silver and Gold row counts and document each filtering rule.
