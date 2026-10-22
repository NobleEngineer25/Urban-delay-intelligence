
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col

builder = (
    SparkSession.builder
    .appName("UrbanDelayBaseline")
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

silver_path = (
    "pipelines/prediction/ingest/src/data/silver/"
    "chicago_taxi"
)

gold_path = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_baseline"
)

# Read Silver data
df = spark.read.format("delta").load(silver_path)

# Keep rows with valid pickup, dropoff, hour and duration
df = df.filter(
    col("pickup_community_area").isNotNull()
    & col("dropoff_community_area").isNotNull()
    & col("hour").isNotNull()
    & col("trip_duration_minutes").isNotNull()
    & (col("trip_duration_minutes") > 0)
)

# Calculate historical mean duration per OD pair and hour
baseline = (
    df.groupBy(
        "pickup_community_area",
        "dropoff_community_area",
        "hour"
    )
    .agg(
        avg("trip_duration_minutes").alias("predicted_duration_minutes"),
        count("*").alias("historical_trip_count")
    )
)

# Save baseline lookup table as Delta
baseline.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("Silver rows used:", df.count())
print("Baseline groups:", baseline.count())

print("Example baseline predictions:")
baseline.orderBy(
    col("historical_trip_count").desc()
).show(10, truncate=False)

print("Gold baseline table saved to:", gold_path)

spark.stop()
