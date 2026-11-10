
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when

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

silver_path = (
    "pipelines/prediction/ingest/src/data/silver/"
    "chicago_taxi"
)

gold_path = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_features"
)

df = spark.read.format("delta").load(silver_path)

# Derive additional features from trip start time.
df = (
    df
    .withColumn(
        "month",
        month(col("trip_start_timestamp")),
    )
    .withColumn(
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
)

# Keep the target separate from the input features.
feature_columns = [
    "trip_id",
    "trip_start_timestamp",
    "pickup_community_area",
    "dropoff_community_area",
    "hour",
    "day_of_week",
    "is_weekend",
    "month",
    "is_rush_hour",
    "trip_duration_minutes",
]

df.select(*feature_columns).write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("Feature engineering completed.")
print("Gold data saved to:", gold_path)
print("Feature rows:", df.count())

df.select(*feature_columns).show(5, truncate=False)

spark.stop()
