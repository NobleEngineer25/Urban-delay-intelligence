
import mlflow
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    abs as spark_abs,
    count,
    hour,
    to_timestamp,
    when,
)

builder = (
    SparkSession.builder
    .appName("EvaluateUrbanDelayBaseline")
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

df = spark.read.format("delta").load(silver_path)

# Keep valid trips and the fields required for evaluation.
df = df.filter(
    col("trip_start_timestamp").isNotNull()
    & col("pickup_community_area").isNotNull()
    & col("dropoff_community_area").isNotNull()
    & col("trip_duration_minutes").isNotNull()
    & (col("trip_duration_minutes") > 0)
)

# Sort by trip start time and split chronologically:
# earliest 80% for training, latest 20% for testing.
df = df.orderBy("trip_start_timestamp")

total_rows = df.count()
split_row = int(total_rows * 0.8)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

ordering = Window.orderBy("trip_start_timestamp")

df = df.withColumn("_row_num", row_number().over(ordering))

train = df.filter(col("_row_num") <= split_row)
test = df.filter(col("_row_num") > split_row)

# Derive hour from the timestamp so the feature is consistent.
train = train.withColumn(
    "hour",
    hour("trip_start_timestamp"),
)
test = test.withColumn(
    "hour",
    hour("trip_start_timestamp"),
)

# Build the lookup table using TRAINING DATA ONLY.
baseline = (
    train.groupBy(
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
    )
    .agg(
        avg("trip_duration_minutes").alias("prediction"),
        count("*").alias("historical_trip_count"),
    )
)

# Match each test trip to its historical average.
test_predictions = test.join(
    baseline,
    on=[
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
    ],
    how="left",
)

# For combinations not found in training, use the overall
# training-set average as a fallback.
training_mean = train.agg(
    avg("trip_duration_minutes").alias("mean_duration")
).first()["mean_duration"]

test_predictions = test_predictions.withColumn(
    "prediction",
    when(
        col("prediction").isNull(),
        training_mean,
    ).otherwise(col("prediction")),
)

# Calculate mean absolute error in minutes.
results = test_predictions.withColumn(
    "absolute_error",
    spark_abs(
        col("trip_duration_minutes") - col("prediction")
    ),
)

mae = results.agg(
    avg("absolute_error").alias("mae")
).first()["mae"]
# Configure MLflow tracking
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

# Log the baseline evaluation
with mlflow.start_run(run_name="historical_mean_baseline"):
    mlflow.log_params({
        "model_type": "historical_mean",
        "target": "trip_duration_minutes",
        "train_test_split": "80/20",
        "split_strategy": "chronological",
        "fallback_strategy": "training_mean",
        "grouping_features": (
            "pickup_community_area,"
            "dropoff_community_area,hour"
        ),
    })

    mlflow.log_metrics({
        "test_mae_minutes": float(mae),
        "training_mean_minutes": float(training_mean),
        "total_valid_trips": float(total_rows),
        "training_trips": float(train.count()),
        "test_trips": float(test.count()),
    })

    mlflow.set_tags({
        "project": "urban-delay-intelligence",
        "city": "Chicago",
        "stage": "baseline",
    })

    print("MLflow run logged successfully.")
    
print("\n===== BASELINE EVALUATION =====")
print(f"Total valid trips: {total_rows}")
print(f"Training trips: {train.count()}")
print(f"Test trips: {test.count()}")
print(f"Training-set fallback mean: {training_mean:.2f} minutes")
print(f"Test MAE: {mae:.2f} minutes")

print("\nExample predictions:")
results.select(
    "trip_start_timestamp",
    "pickup_community_area",
    "dropoff_community_area",
    "trip_duration_minutes",
    "prediction",
    "absolute_error",
).show(10, truncate=False)

spark.stop()
