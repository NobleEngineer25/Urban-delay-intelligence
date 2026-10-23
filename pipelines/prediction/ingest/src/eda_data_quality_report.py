
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
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

spark = (
    SparkSession.builder
    .appName("UrbanDelayEDADataQuality")
    .master("local[*]")
    .getOrCreate()
)


def fmt(value):
    if value is None:
        return "N/A"
    return f"{value:,.3f}"


try:
    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Raw dataset not found: {RAW_FILE}"
        )

    df = spark.read.csv(
        str(RAW_FILE),
        header=True,
        inferSchema=True,
    ).cache()

    row_count = df.count()
    columns = df.columns

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

    # Date range and temporal overview
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
        ])

        lines.extend([
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
            lines.append(f"| {row['hour']} | {row['count']:,} |")

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
            lines.append(f"| {row['date']} | {row['count']:,} |")

    # Missing values
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
            if row_count else 0
        )
        lines.append(
            f"| `{name}` | {missing_count:,} "
            f"| {percentage:.2f}% |"
        )

    # Exact duplicates and trip IDs
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
        missing_ids = df.filter(col("trip_id").isNull()).count()

        lines.extend([
            f"- **Distinct non-null trip IDs:** {distinct_ids:,}",
            f"- **Missing trip IDs:** {missing_ids:,}",
        ])

    # Numeric distributions
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
            f"| {fmt(stats['mean'])} | {fmt(stats['median'])} "
            f"| {fmt(stats['p95'])} | {fmt(stats['max'])} |"
        )

    # Domain-specific quality checks
    lines.extend([
        "",
        "## 6. Data quality checks",
        "",
        "| Check | Result |",
        "|---|---:|",
    ])

    def add_check(label, condition):
        number = df.filter(condition).count()
        lines.append(f"| {label} | {number:,} rows |")

    if "trip_seconds" in columns:
        add_check(
            "Missing trip_seconds",
            col("trip_seconds").isNull(),
        )
        add_check(
            "Zero trip_seconds",
            col("trip_seconds") == 0,
        )
        add_check(
            "Negative trip_seconds",
            col("trip_seconds") < 0,
        )

    if "trip_miles" in columns:
        add_check(
            "Missing trip_miles",
            col("trip_miles").isNull(),
        )
        add_check(
            "Zero trip_miles",
            col("trip_miles") == 0,
        )
        add_check(
            "Negative trip_miles",
            col("trip_miles") < 0,
        )

    if {
        "trip_start_timestamp",
        "trip_end_timestamp",
    }.issubset(columns):
        add_check(
            "Missing trip end timestamp",
            col("trip_end_timestamp").isNull(),
        )
        add_check(
            "Trip end before trip start",
            col("trip_end_timestamp")
            < col("trip_start_timestamp"),
        )

    # Categorical distributions
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
                else str(row[name]).replace("|", "\\|")
            )
            percentage = (
                row["count"] / row_count * 100
                if row_count else 0
            )
            lines.append(
                f"| {value} | {row['count']:,} "
                f"| {percentage:.2f}% |"
            )

        lines.append("")

    # Extreme values: inspect top records, don't automatically remove them
    lines.extend([
        "",
        "## 8. Extreme-value investigation",
        "",
        "These are the highest observed values, not automatically "
        "confirmed errors.",
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

    for name in extreme_columns:
        if name not in columns:
            continue

        lines.extend([
            f"### Highest values for `{name}`",
            "",
        ])

        identifiers = [
            c for c in [
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
            if c in columns
        ]

        top_rows = (
            df.filter(col(name).isNotNull())
            .orderBy(col(name).desc())
            .select(*identifiers)
            .limit(5)
            .collect()
        )

        lines.append(
            "| " + " | ".join(identifiers) + " |"
        )
        lines.append(
            "|" + "|".join(["---"] * len(identifiers)) + "|"
        )

        for row in top_rows:
            values = []
            for field in identifiers:
                value = row[field]
                values.append(
                    "NULL" if value is None
                    else str(value).replace("|", "\\|")
                )
            lines.append("| " + " | ".join(values) + " |")

        lines.append("")

    # Summary and limitations
    lines.extend([
        "",
        "## 9. Interpretation and limitations",
        "",
        "- Missing geographic fields may limit spatial analysis.",
        "- Zero or negative durations should be investigated separately.",
        "- Extreme distance and fare values need domain validation.",
        "- Exact duplicate checks do not replace business-key checks.",
        "- No source rows are changed or deleted by this script.",
        "- These findings describe the raw CSV before Bronze ingestion.",
        "- Follow-up checks should compare raw, Bronze, Silver and Gold "
        "row counts and document each filtering rule.",
        "",
    ])

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")

    print(f"Report saved to: {REPORT_FILE}")
    print(f"Rows: {row_count:,}; columns: {len(columns)}")

finally:
    spark.stop()
