
from pathlib import Path
from delta import configure_spark_with_delta_pip
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_format,
    hour,
    isnan,
    max as spark_max,
    mean,
    min as spark_min,
    percentile_approx,
    sum as spark_sum,
    to_date,
    when,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[4]

RAW_FILE = (
    PROJECT_ROOT
    / "pipelines"
    / "prediction"
    / "ingest"
    / "src"
    / "data"
    / "raw"
    / "chicago_taxi_2023_01.csv"
)

REPORT_FILE = (
    PROJECT_ROOT
    / "reports"
    / "eda_data_quality_report.md"
)
BRONZE_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "prediction"
    / "ingest"
    / "src"
    / "data"
    / "bronze"
    / "chicago_taxi"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "prediction"
    / "ingest"
    / "src"
    / "data"
    / "silver"
    / "chicago_taxi"
)

FIGURES_DIR = REPORT_FILE.parent / "figures"


# --------------------------------------------------
# Spark setup and helper functions
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("UrbanDelayEDADataQuality")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()

def fmt(value):
    """Format numeric values for the Markdown report."""
    if value is None:
        return "N/A"
    return f"{value:,.3f}"


def add_check(df, lines, label, condition):
    """Append the number of rows matching a quality check."""
    number = df.filter(condition).count()
    lines.append(f"| {label} | {number:,} rows |")


def escape_markdown(value):
    """Prevent pipe characters from breaking Markdown tables."""
    if value is None:
        return "NULL"
    return str(value).replace("|", r"\|").replace("\n", " ")


try:
    # --------------------------------------------------
    # 1. Load raw data
    # --------------------------------------------------

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw dataset not found: {RAW_FILE}"
        )

    df = (
        spark.read.csv(
            str(RAW_FILE),
            header=True,
            inferSchema=True,
        )
        .cache()
    )

    row_count = df.count()
    columns = df.columns

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# EDA and Data Quality Report",
        "",
        "## 1. Dataset overview",
        "",
        f"- **Source:** `{RAW_FILE.name}`",
        f"- **Rows:** {row_count:,}",
        f"- **Columns:** {len(columns)}",
        "",
        "### Schema",
        "",
        "| Column | Data type |",
        "|---|---|",
    ]

    for field in df.schema.fields:
        lines.append(
            f"| `{field.name}` | "
            f"`{field.dataType.simpleString()}` |"
        )

    # --------------------------------------------------
    # 2. Time coverage
    # --------------------------------------------------

    lines.extend([
        "",
        "## 2. Time coverage",
        "",
    ])

    if "trip_start_timestamp" in columns:
        time_stats = df.agg(
            spark_min("trip_start_timestamp").alias("first"),
            spark_max("trip_start_timestamp").alias("last"),
        ).first()

        lines.extend([
            f"- **First trip start:** {time_stats['first']}",
            f"- **Last trip start:** {time_stats['last']}",
            "",
            "### Trips by hour of day",
            "",
            "| Hour | Trips |",
            "|---:|---:|",
        ])

        hourly = (
            df.filter(col("trip_start_timestamp").isNotNull())
            .groupBy(hour("trip_start_timestamp").alias("hour"))
            .count()
            .orderBy("hour")
            .collect()
        )

        for row in hourly:
            lines.append(
                f"| {row['hour']} | {row['count']:,} |"
            )

        # Calculate the busiest hours once, outside the loop.
        busiest_hours = sorted(
            hourly,
            key=lambda row: row["count"],
            reverse=True,
        )[:5]

        lines.extend([
            "",
            "### Top 5 busiest hours",
            "",
            "| Rank | Hour | Trips |",
            "|---:|---:|---:|",
        ])

        for rank, row in enumerate(busiest_hours, start=1):
            lines.append(
                f"| {rank} | {row['hour']}:00 | "
                f"{row['count']:,} |"
            )

        lines.extend([
            "",
            "### Trips by date",
            "",
            "| Date | Trips |",
            "|---|---:|",
        ])

        daily = (
            df.filter(col("trip_start_timestamp").isNotNull())
            .groupBy(
                to_date("trip_start_timestamp").alias("date")
            )
            .count()
            .orderBy("date")
            .collect()
        )

        for row in daily:
            lines.append(
                f"| {row['date']} | {row['count']:,} |"
            )

        lines.extend([
            "",
            "### Trips by day of week",
            "",
            "| Day | Trips |",
            "|---|---:|",
        ])

        weekdays = (
            df.filter(col("trip_start_timestamp").isNotNull())
            .groupBy(
                date_format(
                    col("trip_start_timestamp"), "EEEE"
                ).alias("day")
            )
            .count()
            .orderBy("day")
            .collect()
        )

        for row in weekdays:
            lines.append(
                f"| {row['day']} | {row['count']:,} |"
            )

    # --------------------------------------------------
    # 3. Missing values
    # --------------------------------------------------

    lines.extend([
        "",
        "## 3. Missing values",
        "",
        "| Column | Missing/null/NaN | Percentage |",
        "|---|---:|---:|",
    ])

    missing_exprs = []

    for name in columns:
        dtype = df.schema[name].dataType.simpleString()
        condition = col(name).isNull()

        if dtype in ("float", "double"):
            condition = condition | isnan(col(name))

        missing_exprs.append(
            spark_sum(
                when(condition, 1).otherwise(0)
            ).alias(name)
        )

    missing_row = df.agg(*missing_exprs).first()

    for name in columns:
        missing_count = int(missing_row[name] or 0)
        percentage = (
            missing_count / row_count * 100
            if row_count
            else 0
        )

        lines.append(
            f"| `{name}` | {missing_count:,} "
            f"| {percentage:.2f}% |"
        )

    # --------------------------------------------------
    # 4. Duplicate records
    # --------------------------------------------------

    duplicate_count = row_count - df.dropDuplicates().count()

    lines.extend([
        "",
        "## 4. Duplicate records",
        "",
        f"- **Exact duplicate rows:** {duplicate_count:,}",
    ])

    if "trip_id" in columns:
        distinct_ids = df.select(
            countDistinct("trip_id")
        ).first()[0]

        missing_ids = df.filter(
            col("trip_id").isNull()
        ).count()

        lines.extend([
            f"- **Distinct non-null trip IDs:** {distinct_ids:,}",
            f"- **Missing trip IDs:** {missing_ids:,}",
        ])

    # --------------------------------------------------
    # 5. Numeric distributions
    # --------------------------------------------------

    numeric_types = {
        "int", "bigint", "float", "double",
        "smallint", "tinyint", "long", "decimal",
    }

    numeric_fields = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() in numeric_types
    ]

    lines.extend([
        "",
        "## 5. Numeric distributions",
        "",
        "| Column | Min | Mean | Median | P95 | Max |",
        "|---|---:|---:|---:|---:|---:|",
    ])

    for name in numeric_fields:
        stats = df.agg(
            spark_min(col(name)).alias("min"),
            mean(col(name)).alias("mean"),
            percentile_approx(col(name), 0.50).alias("median"),
            percentile_approx(col(name), 0.95).alias("p95"),
            spark_max(col(name)).alias("max"),
        ).first()

        lines.append(
            f"| `{name}` | {fmt(stats['min'])} "
            f"| {fmt(stats['mean'])} "
            f"| {fmt(stats['median'])} "
            f"| {fmt(stats['p95'])} "
            f"| {fmt(stats['max'])} |"
        )

    # --------------------------------------------------
    # 6. Data quality checks
    # --------------------------------------------------

    lines.extend([
        "",
        "## 6. Data quality checks",
        "",
        "| Check | Result |",
        "|---|---:|",
    ])

    if "trip_seconds" in columns:
        add_check(
            df, lines, "Missing trip_seconds",
            col("trip_seconds").isNull(),
        )
        add_check(
            df, lines, "Zero trip_seconds",
            col("trip_seconds") == 0,
        )
        add_check(
            df, lines, "Negative trip_seconds",
            col("trip_seconds") < 0,
        )

    if "trip_miles" in columns:
        add_check(
            df, lines, "Missing trip_miles",
            col("trip_miles").isNull(),
        )
        add_check(
            df, lines, "Zero trip_miles",
            col("trip_miles") == 0,
        )
        add_check(
            df, lines, "Negative trip_miles",
            col("trip_miles") < 0,
        )

    if {
        "trip_start_timestamp",
        "trip_end_timestamp",
    }.issubset(columns):
        add_check(
            df, lines, "Missing trip end timestamp",
            col("trip_end_timestamp").isNull(),
        )
        add_check(
            df, lines, "Trip end before trip start",
            col("trip_end_timestamp")
            < col("trip_start_timestamp"),
        )

    # --------------------------------------------------
    # 7. Categorical distributions
    # --------------------------------------------------

    lines.extend([
        "",
        "## 7. Categorical distributions",
        "",
    ])

    for name in ["payment_type", "company"]:
        if name not in columns:
            continue

        lines.extend([
            f"### {name}",
            "",
            "| Value | Count | Percentage |",
            "|---|---:|---:|",
        ])

        categories = (
            df.groupBy(name)
            .count()
            .orderBy(col("count").desc())
            .collect()
        )

        for row in categories:
            value = (
                "(NULL)" if row[name] is None
                else escape_markdown(row[name])
            )
            percentage = (
                row["count"] / row_count * 100
                if row_count
                else 0
            )

            lines.append(
                f"| {value} | {row['count']:,} "
                f"| {percentage:.2f}% |"
            )

        lines.append("")

    # --------------------------------------------------
    # 8. Geographic analysis
    # --------------------------------------------------

    lines.extend([
        "",
        "## 8. Geographic analysis",
        "",
        "Trips by pickup and dropoff community area.",
        "",
    ])

    for name in [
        "pickup_community_area",
        "dropoff_community_area",
    ]:
        if name not in columns:
            continue

        lines.extend([
            f"### Top 10: `{name}`",
            "",
            "| Community area | Trips | Percentage of all trips |",
            "|---:|---:|---:|",
        ])

        areas = (
            df.filter(col(name).isNotNull())
            .groupBy(name)
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        )

        for row in areas:
            percentage = (
                row["count"] / row_count * 100
                if row_count
                else 0
            )

            lines.append(
                f"| {row[name]} | {row['count']:,} "
                f"| {percentage:.2f}% |"
            )

        lines.append("")

    # --------------------------------------------------
    # 9. Extreme-value investigation
    # --------------------------------------------------

    lines.extend([
        "",
        "## 9. Extreme-value investigation",
        "",
        "These are the highest observed values, not automatically "
        "confirmed errors. Values require domain validation.",
        "",
    ])

    extreme_columns = [
        "trip_seconds",
        "trip_miles",
        "fare",
        "tolls",
        "extras",
        "trip_total",
    ]

    identifiers = [
        name for name in [
            "trip_id",
            "trip_start_timestamp",
            "trip_end_timestamp",
            "trip_seconds",
            "trip_miles",
            "fare",
            "tolls",
            "extras",
            "trip_total",
        ]
        if name in columns
    ]

    for name in extreme_columns:
        if name not in columns:
            continue

        lines.extend([
            f"### Highest values for `{name}`",
            "",
            "| " + " | ".join(identifiers) + " |",
            "|" + "|".join(["---"] * len(identifiers)) + "|",
        ])

        top_rows = (
            df.filter(col(name).isNotNull())
            .orderBy(col(name).desc())
            .select(*identifiers)
            .limit(5)
            .collect()
        )

        for row in top_rows:
            values = [
                escape_markdown(row[field])
                for field in identifiers
            ]
            lines.append("| " + " | ".join(values) + " |")

        lines.append("")

    # --------------------------------------------------
    # 10. Distribution visualizations
    # --------------------------------------------------

    lines.extend([
        "",
        "## 10. Distribution visualizations",
        "",
        "Histograms exclude missing, negative and out-of-range values.",
        "",
    ])

    plot_configs = [
        {
            "column": "trip_seconds",
            "title": "Trip Duration Distribution",
            "xlabel": "Trip duration (minutes)",
            "filename": "trip_duration_distribution.png",
            "scale": 60,
            "max_value": 120,
            "unit": "minutes",
        },
        {
            "column": "trip_miles",
            "title": "Trip Distance Distribution",
            "xlabel": "Trip distance (miles)",
            "filename": "trip_distance_distribution.png",
            "scale": 1,
            "max_value": 100,
            "unit": "miles",
        },
    ]

    for config in plot_configs:
        name = config["column"]

        if name not in columns:
            continue

        values = (
            df.filter(
                col(name).isNotNull()
                & (col(name) >= 0)
                & (
                    col(name)
                    <= config["max_value"] * config["scale"]
                )
            )
            .select(col(name))
            .rdd.map(
                lambda row: row[0] / config["scale"]
            )
            .collect()
        )

        if not values:
            continue

        plt.figure(figsize=(10, 5))
        plt.hist(values, bins=50, edgecolor="black")
        plt.title(config["title"])
        plt.xlabel(config["xlabel"])
        plt.ylabel("Number of trips")
        plt.tight_layout()

        image_path = FIGURES_DIR / config["filename"]
        plt.savefig(image_path, dpi=150)
        plt.close()

        lines.extend([
            f"### {config['title']}",
            "",
            f"![{config['title']}](figures/{config['filename']})",
            "",
            f"*Plot includes non-negative values up to "
            f"{config['max_value']} {config['unit']}.*",
            "",
        ])

    # --------------------------------------------------
    # 11. Geographic visualization
    # --------------------------------------------------

    if {
        "pickup_centroid_latitude",
        "pickup_centroid_longitude",
    }.issubset(columns):
        geo_df = (
            df.filter(
                col("pickup_centroid_latitude").isNotNull()
                & col("pickup_centroid_longitude").isNotNull()
                & col("pickup_centroid_latitude").between(
                    41.6, 42.1
                )
                & col("pickup_centroid_longitude").between(
                    -88.0, -87.4
                )
            )
            .groupBy(
                "pickup_centroid_latitude",
                "pickup_centroid_longitude",
            )
            .count()
            .collect()
        )

        if geo_df:
            latitudes = [
                row["pickup_centroid_latitude"]
                for row in geo_df
            ]
            longitudes = [
                row["pickup_centroid_longitude"]
                for row in geo_df
            ]
            counts = [row["count"] for row in geo_df]

            plt.figure(figsize=(9, 8))
            plt.scatter(
                longitudes,
                latitudes,
                s=[min(count, 500) for count in counts],
                alpha=0.6,
            )
            plt.title("Chicago Taxi Pickup Density")
            plt.xlabel("Longitude")
            plt.ylabel("Latitude")
            plt.tight_layout()

            geo_filename = "pickup_density_chicago.png"
            plt.savefig(FIGURES_DIR / geo_filename, dpi=150)
            plt.close()

            lines.extend([
                "",
                "## 11. Geographic visualization",
                "",
                "Pickup locations are grouped by their recorded "
                "centroid coordinates. Point sizes represent trip "
                "counts, with marker sizes capped for readability.",
                "",
                f"![Chicago pickup density](figures/{geo_filename})",
                "",
                f"- **Distinct pickup coordinates:** {len(geo_df):,}",
                "",
                "The geographic bounds are restricted to the "
                "Chicago area; coordinates outside those bounds "
                "are excluded from this plot only.",
                "",
            ])

    # --------------------------------------------------
    # 12. Interpretation and limitations
    # --------------------------------------------------

    lines.extend([
        "",
        "## 12. Interpretation and limitations",
        "",
        "- Missing geographic fields may limit spatial analysis.",
        "- Zero or negative durations should be investigated.",
        "- Extreme distance and fare values need domain validation.",
        "- Exact duplicate checks do not replace business-key checks.",
        "- This script does not change or delete source rows.",
        "- Findings describe the raw CSV before Bronze ingestion.",
        "- Compare raw, Bronze, Silver and Gold row counts and "
        "document each filtering rule.",
        "- Histogram limits and geographic bounds affect only "
        "the visualizations, not the underlying dataset.",
        "",
    ])
    # --------------------------------------------------
    # 13. Bronze vs Silver data quality comparison
    # --------------------------------------------------

    lines.extend([
        "",
        "## 13. Bronze vs Silver data quality comparison",
        "",
        "Silver applies provisional data-quality rules to Bronze.",
        "",
        "| Metric | Bronze | Silver |",
        "|---|---:|---:|",
    ])

    bronze_df = spark.read.format("delta").load(str(BRONZE_PATH))
    silver_df = spark.read.format("delta").load(str(SILVER_PATH))

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    removed_count = bronze_count - silver_count
    retained_pct = (
        silver_count / bronze_count * 100
        if bronze_count
        else 0
    )

    lines.extend([
        f"| Total rows | {bronze_count:,} | {silver_count:,} |",
        f"| Missing trip IDs | "
        f"{bronze_df.filter(col('trip_id').isNull()).count():,} | "
        f"{silver_df.filter(col('trip_id').isNull()).count():,} |",
        f"| Zero trip duration | "
        f"{bronze_df.filter(col('trip_seconds') == 0).count():,} | "
        f"{silver_df.filter(col('trip_seconds') == 0).count():,} |",
        f"| Zero trip distance | "
        f"{bronze_df.filter(col('trip_miles') == 0).count():,} | "
        f"{silver_df.filter(col('trip_miles') == 0).count():,} |",
        f"| Negative trip duration | "
        f"{bronze_df.filter(col('trip_seconds') < 0).count():,} | "
        f"{silver_df.filter(col('trip_seconds') < 0).count():,} |",
        "",
        "### Filtering summary",
        "",
        f"- **Bronze rows:** {bronze_count:,}",
        f"- **Silver rows:** {silver_count:,}",
        f"- **Rows removed:** {removed_count:,}",
        f"- **Rows retained:** {retained_pct:.2f}%",
        "",
        "### Silver quality rules",
        "",
        "- Required fields must not be null.",
        "- Trip duration must be greater than 0 and at most 10,800 seconds.",
        "- Trip distance must be greater than 0 and at most 100 miles.",
        "- Trip total must be non-negative.",
        "- Calculated average speed must not exceed 100 mph.",
        "- Trip duration is derived from `trip_seconds`.",
        "",
        "These are project-specific provisional thresholds, "
        "not universal definitions of valid taxi trips.",
        "",
    ])
    # --------------------------------------------------
    # Write report
    # --------------------------------------------------

    REPORT_FILE.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )

    print(f"Report saved to: {REPORT_FILE}")
    print(f"Rows: {row_count:,}; columns: {len(columns)}")
    print(f"Figures saved to: {FIGURES_DIR}")

finally:
    spark.stop()
