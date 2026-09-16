from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    min,
    max,
    avg
)


spark = (
    SparkSession.builder
    .appName("TaxiDataQualityCheck")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    "data/processed/chicago_taxi_2023_01"
)


print("=== BASIC INFO ===")

print("Rows:", df.count())

print("\nColumns:")
print(df.columns)


# --------------------------------------------------
# Missing values
# --------------------------------------------------

print("\n=== MISSING VALUES ===")

missing = df.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
])

missing.show(truncate=False)


# --------------------------------------------------
# Numeric statistics
# --------------------------------------------------

print("\n=== TRIP STATISTICS ===")

df.select(
    min("trip_duration_minutes").alias("min_duration"),
    max("trip_duration_minutes").alias("max_duration"),
    avg("trip_duration_minutes").alias("avg_duration"),
    min("trip_miles").alias("min_miles"),
    max("trip_miles").alias("max_miles"),
    avg("trip_miles").alias("avg_miles"),
    min("avg_speed_mph").alias("min_speed"),
    max("avg_speed_mph").alias("max_speed"),
    avg("avg_speed_mph").alias("avg_speed")
).show()


# --------------------------------------------------
# Time distribution
# --------------------------------------------------

print("\n=== TRIPS BY HOUR ===")

df.groupBy("hour") \
    .count() \
    .orderBy("hour") \
    .show(24)


print("\n=== TRIPS BY DAY OF WEEK ===")

df.groupBy("day_of_week") \
    .count() \
    .orderBy("day_of_week") \
    .show()


spark.stop()