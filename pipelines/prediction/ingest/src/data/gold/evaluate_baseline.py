
import mlflow
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    avg,
    col,
    count,
    hour,
    row_number,
    when,
)
from pyspark.sql.window import Window

# -----------------------------
# 1. Create Spark session
# -----------------------------
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


# -----------------------------
# 2. Read Gold dataset
# -----------------------------
gold_path = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_features"
)

df = spark.read.format("delta").load(gold_path)


# -----------------------------
# 3. Apply same initial filters
# -----------------------------
df = df.filter(
    col("trip_start_timestamp").isNotNull()
    & col("pickup_community_area").isNotNull()
    & col("dropoff_community_area").isNotNull()
    & col("trip_duration_minutes").isNotNull()
    & (col("trip_duration_minutes") > 0)
)


# -----------------------------
# 4. Same chronological split
# -----------------------------
ordering = Window.orderBy(
    "trip_start_timestamp",
    "trip_id",
)

df = df.withColumn(
    "_row_num",
    row_number().over(ordering),
)

total_rows = df.count()
split_row = int(total_rows * 0.8)

train = df.filter(col("_row_num") <= split_row)
test = df.filter(col("_row_num") > split_row)


# -----------------------------
# 5. Same feature filtering as LightGBM
# -----------------------------
feature_columns = [
    "pickup_community_area",
    "dropoff_community_area",
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "is_weekend",
    "is_rush_hour",
    "pickup_dropoff_distance_km",
]

train = train.dropna(
    subset=feature_columns + ["trip_duration_minutes"]
)

test = test.dropna(
    subset=feature_columns + ["trip_duration_minutes"]
)


# -----------------------------
# 6. Derive hour for baseline
# -----------------------------
train = train.withColumn(
    "hour",
    hour("trip_start_timestamp"),
)

test = test.withColumn(
    "hour",
    hour("trip_start_timestamp"),
)


# -----------------------------
# 7. Build baseline using training data only
# -----------------------------
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


# -----------------------------
# 8. Generate test predictions
# -----------------------------
test_predictions = test.join(
    baseline,
    on=[
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
    ],
    how="left",
)


# Fallback for unseen combinations
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


# -----------------------------
# 9. Evaluate MAE
# -----------------------------
results = test_predictions.withColumn(
    "absolute_error",
    spark_abs(
        col("trip_duration_minutes") - col("prediction")
    ),
)

mae = results.agg(
    avg("absolute_error").alias("mae")
).first()["mae"]


# -----------------------------
# 10. Log results to MLflow
# -----------------------------
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

with mlflow.start_run(run_name="historical_mean_baseline_gold"):

    mlflow.log_params({
        "model_type": "historical_mean",
        "dataset": "chicago_taxi_features",
        "target": "trip_duration_minutes",
        "train_test_split": "80/20",
        "split_strategy": "chronological",
        "fallback_strategy": "training_mean",
        "grouping_features": (
            "pickup_community_area,"
            "dropoff_community_area,hour"
        ),
        "feature_filtering": "same_as_lightgbm",
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


# -----------------------------
# 11. Print evaluation results
# -----------------------------
print("\n===== BASELINE EVALUATION =====")
print("Dataset: Gold")
print("Total rows before feature filtering:", total_rows)
print("Training rows after feature filtering:", train.count())
print("Test rows after feature filtering:", test.count())
print(f"Training-set fallback mean: {training_mean:.2f} minutes")
print(f"Baseline test MAE: {mae:.2f} minutes")

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
