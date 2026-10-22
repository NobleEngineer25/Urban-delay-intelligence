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

# Read Bronze Delta data
bronze_path = "pipelines/prediction/ingest/src/data/bronze/chicago_taxi"
df = spark.read.format("delta").load(bronze_path)
print("Kolonner i Bronze:")
print(df.columns)

print("Bronze rows:", df.count())

# Remove duplicate trips
df = df.dropDuplicates(["trip_id"])

# Remove rows with missing or invalid values
required_columns = [
    "trip_id",
    "trip_start_timestamp",
    "trip_seconds",
    "trip_miles",
    "trip_total",
]

df = df.dropna(subset=required_columns)

df = df.filter(
    (col("trip_seconds") > 0)
    & (col("trip_miles") > 0)
    & (col("trip_total") >= 0)
)

# Create features for analysis and prediction
df = (
    df
    .withColumn(
        "trip_duration_minutes",
        col("trip_seconds") / 60
    )
    .withColumn(
        "avg_speed_mph",
        col("trip_miles") / (col("trip_seconds") / 3600)
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

# Save cleaned data as Silver Delta
silver_path = "pipelines/prediction/ingest/src/data/silver/chicago_taxi"
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver rows:", df.count())
print("Silver Delta table saved to:", silver_path)

spark.stop()
