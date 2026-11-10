
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, when

builder = (
    SparkSession.builder
    .appName("UrbanDelaySilver")
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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    # Read Bronze Delta data
    bronze_path = "pipelines/prediction/ingest/src/data/bronze/chicago_taxi"
    df = spark.read.format("delta").load(bronze_path)

    bronze_count = df.count()
    print("Bronze rows:", bronze_count)
    print("Bronze columns:", df.columns)

    # Remove duplicate trip IDs
    df = df.dropDuplicates(["trip_id"])

    # Remove rows with missing required values
    required_columns = [
        "trip_id",
        "trip_start_timestamp",
        "trip_seconds",
        "trip_miles",
        "trip_total",
    ]
    df = df.dropna(subset=required_columns)

    # Apply provisional data-quality rules
    df = df.filter(
        (col("trip_seconds") > 0)
        & (col("trip_seconds") <= 10800)
        & (col("trip_miles") > 0)
        & (col("trip_miles") <= 100)
        & (col("trip_total") >= 0)
    )

    # Calculate speed and exclude implausible values
    df = df.withColumn(
        "avg_speed_mph",
        col("trip_miles") / (col("trip_seconds") / 3600)
    )
    df = df.filter(col("avg_speed_mph") <= 100)

    # Create features
    df = (
        df
        .withColumn(
            "trip_duration_minutes",
            col("trip_seconds") / 60
        )
        .withColumn(
            "hour",
            hour(col("trip_start_timestamp"))
        )
        .withColumn(
            "day_of_week",
            dayofweek(col("trip_start_timestamp"))
        )
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin(1, 7), 1).otherwise(0)
        )
    )

    silver_count = df.count()

    print("Silver rows:", silver_count)
    print("Rows removed:", bronze_count - silver_count)
    print(
        "Rows retained:",
        f"{silver_count / bronze_count * 100:.2f}%"
    )

    # Save cleaned data as Silver Delta
    silver_path = "pipelines/prediction/ingest/src/data/silver/chicago_taxi"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)

    print("Silver Delta table saved to:", silver_path)

finally:
    spark.stop()
