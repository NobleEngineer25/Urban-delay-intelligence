from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    round,
    when,
)

spark = (
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
    .getOrCreate()
)

# 1. Read Bronze Delta table
bronze_path = "data/bronze/chicago_taxi"
df = spark.read.format("delta").load(bronze_path)

print("Bronze rows:", df.count())

# 2. Remove API metadata (only columns that exist)
columns_to_remove = [
    ":id",
    ":version",
    ":created_at",
    ":updated_at",
    ":@computed_region_vrxf_vc4k",
]

existing_columns = [
    name for name in columns_to_remove
    if name in df.columns
]

df = df.drop(*existing_columns)

# 3. Remove duplicates and rows without essential values
df = df.dropDuplicates(["trip_id"])

required_columns = [
    "trip_id",
    "trip_start_timestamp",
    "trip_seconds",
    "trip_miles",
    "trip_total",
]

df = df.dropna(subset=required_columns)

# 4. Remove invalid trips
df = df.filter(
    (col("trip_seconds") > 0)
    & (col("trip_seconds") <= 10800)
    & (col("trip_miles") > 0)
    & (col("trip_miles") <= 100)
    & (col("trip_total") >= 0)
)

# 5. Create trip duration in minutes
df = df.withColumn(
    "trip_duration_minutes",
    round(col("trip_seconds") / 60, 2)
)

# 6. Create average speed in mph
df = df.withColumn(
    "avg_speed_mph",
    round(
        col("trip_miles") / (col("trip_seconds") / 3600),
        2,
    ),
)

df = df.filter(
    (col("avg_speed_mph") > 0)
    & (col("avg_speed_mph") <= 100)
)

# 7. Create time features
df = df.withColumn(
    "hour",
    hour(col("trip_start_timestamp"))
)

df = df.withColumn(
    "day_of_week",
    dayofweek(col("trip_start_timestamp"))
)

df = df.withColumn(
    "is_weekend",
    when(col("day_of_week").isin(1, 7), 1).otherwise(0)
)

# 8. Inspect cleaned data
print("\n=== SILVER DATA ===")
df.printSchema()

df.select(
    "trip_id",
    "trip_start_timestamp",
    "trip_seconds",
    "trip_miles",
    "trip_duration_minutes",
    "avg_speed_mph",
    "hour",
    "day_of_week",
    "is_weekend",
).show(10, truncate=False)

print("Silver rows:", df.count())

# 9. Save as Silver Delta table
silver_path = "data/silver/chicago_taxi"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver Delta table saved to:", silver_path)

spark.stop()
