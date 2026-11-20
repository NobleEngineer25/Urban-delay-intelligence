
import math

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    asin,
    col,
    cos,
    greatest,
    least,
    lit,
    month,
    pow,
    radians,
    sin,
    sqrt,
    when,
)

builder = (
    SparkSession.builder
    .appName("UrbanDelayFeatureEngineering")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

h3_path = (
    "pipelines/prediction/ingest/src/data/silver/"
    "chicago_taxi_h3"
)

gold_path = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_features"
)

# Read Silver data with H3 indexes.
df = spark.read.format("delta").load(h3_path)

# Derive calendar features.
df = df.withColumn(
    "month",
    month(col("trip_start_timestamp")),
)

df = df.withColumn(
    "is_rush_hour",
    when(
        (
            (col("hour") >= 7)
            & (col("hour") <= 9)
        )
        | (
            (col("hour") >= 16)
            & (col("hour") <= 18)
        ),
        1,
    ).otherwise(0),
)

# Cyclical time features.
hour_angle = 2 * math.pi * col("hour") / 24

day_angle = (
    2 * math.pi * (col("day_of_week") - 1) / 7
)

month_angle = (
    2 * math.pi * (col("month") - 1) / 12
)

df = (
    df
    .withColumn("hour_sin", sin(hour_angle))
    .withColumn("hour_cos", cos(hour_angle))
    .withColumn("day_of_week_sin", sin(day_angle))
    .withColumn("day_of_week_cos", cos(day_angle))
    .withColumn("month_sin", sin(month_angle))
    .withColumn("month_cos", cos(month_angle))
)

# Calculate straight-line pickup-to-dropoff distance
# using the Haversine formula.
lat1 = radians(col("pickup_centroid_latitude"))
lon1 = radians(col("pickup_centroid_longitude"))
lat2 = radians(col("dropoff_centroid_latitude"))
lon2 = radians(col("dropoff_centroid_longitude"))

delta_lat = lat2 - lat1
delta_lon = lon2 - lon1

haversine_a = (
    pow(sin(delta_lat / 2), 2)
    + cos(lat1) * cos(lat2)
    * pow(sin(delta_lon / 2), 2)
)

haversine_a = least(
    lit(1.0),
    greatest(lit(0.0), haversine_a),
)

distance_km = (
    2 * 6371.0 * asin(sqrt(haversine_a))
)

df = df.withColumn(
    "pickup_dropoff_distance_km",
    distance_km,
)

# Keep identifiers, target, model features and H3 indexes.
feature_columns = [
    "trip_id",
    "trip_start_timestamp",
    "pickup_community_area",
    "dropoff_community_area",
    "pickup_h3",
    "dropoff_h3",
    "hour",
    "day_of_week",
    "is_weekend",
    "month",
    "is_rush_hour",
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "month_sin",
    "month_cos",
    "pickup_dropoff_distance_km",
    "trip_duration_minutes",
]

gold_df = df.select(*feature_columns)

# Save Gold Delta dataset.
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("Feature engineering completed.")
print("Gold data saved to:", gold_path)
print("Feature rows:", gold_df.count())

gold_df.printSchema()

gold_df.select(
    "trip_id",
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "pickup_dropoff_distance_km",
    "pickup_h3",
    "dropoff_h3",
    "trip_duration_minutes",
).show(10, truncate=False)

spark.stop()
