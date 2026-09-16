from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    when,
    round
)


spark = (
    SparkSession.builder
    .appName("TransformChicagoTaxiData")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 1. Read raw data
# --------------------------------------------------

df = spark.read.csv(
    "src/data/raw/chicago_taxi_2023_01.csv",
    header=True,
    inferSchema=True
)

print("Raw rows:", df.count())


# --------------------------------------------------
# 2. Remove API metadata
# --------------------------------------------------

columns_to_remove = [
    ":id",
    ":version",
    ":created_at",
    ":updated_at",
    ":@computed_region_vrxf_vc4k"
]

df = df.drop(*columns_to_remove)


# --------------------------------------------------
# 3. Remove duplicates
# --------------------------------------------------

df = df.dropDuplicates(["trip_id"])


# --------------------------------------------------
# 4. Remove invalid trips
# --------------------------------------------------

df = df.filter(
    (col("trip_seconds") > 0) &
    (col("trip_seconds") <= 10800) &
    (col("trip_miles") > 0) &
    (col("trip_miles") <= 100) &
    (col("trip_total") >= 0)
)

# --------------------------------------------------
# 5. Create trip duration in minutes
# --------------------------------------------------

df = df.withColumn(
    "trip_duration_minutes",
    round(col("trip_seconds") / 60, 2)
)


# --------------------------------------------------
# 6. Create average speed
# --------------------------------------------------

df = df.withColumn(
    "avg_speed_mph",
    round(
        col("trip_miles") /
        (col("trip_seconds") / 3600),
        2
    )
)

df = df.filter(
    (col("avg_speed_mph") > 0) &
    (col("avg_speed_mph") <= 100)
)
# --------------------------------------------------
# 7. Create time features
# --------------------------------------------------

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


# --------------------------------------------------
# 8. Show result
# --------------------------------------------------

print("\n=== CLEANED DATA ===")

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
    "is_weekend"
).show(10, truncate=False)


print("\nCleaned rows:", df.count())


# --------------------------------------------------
# 9. Save as Parquet
# --------------------------------------------------

output_path = "data/processed/chicago_taxi_2023_01"

df.write.mode("overwrite").parquet(output_path)

print("\nSaved to:", output_path)


spark.stop()