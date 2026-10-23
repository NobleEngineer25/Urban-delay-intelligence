
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

import lightgbm as lgb
import numpy as np
import mlflow
from sklearn.metrics import mean_absolute_error


# -----------------------------
# 1. Create Spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("UrbanDelayLightGBMQuantile")
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

gold_path = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_features"
)

df = spark.read.format("delta").load(gold_path)

# Keep valid rows
df = df.filter(
    col("trip_start_timestamp").isNotNull()
    & col("pickup_community_area").isNotNull()
    & col("dropoff_community_area").isNotNull()
    & col("trip_duration_minutes").isNotNull()
    & (col("trip_duration_minutes") > 0)
)

# -----------------------------
# 2. Chronological train/test split
# -----------------------------
ordering = Window.orderBy("trip_start_timestamp", "trip_id")

df = df.withColumn("_row_num", row_number().over(ordering))

total_rows = df.count()
split_row = int(total_rows * 0.8)

train = df.filter(col("_row_num") <= split_row)
test = df.filter(col("_row_num") > split_row)

feature_columns = [
    "pickup_community_area",
    "dropoff_community_area",
    "hour",
    "day_of_week",
    "is_weekend",
    "month",
    "is_rush_hour",
]

train = train.dropna(subset=feature_columns + ["trip_duration_minutes"])
test = test.dropna(subset=feature_columns + ["trip_duration_minutes"])

X_train = train.select(feature_columns).toPandas()
y_train = train.select("trip_duration_minutes").toPandas().values.ravel()

X_test = test.select(feature_columns).toPandas()
y_test = test.select("trip_duration_minutes").toPandas().values.ravel()

print("Training rows:", len(X_train))
print("Test rows:", len(X_test))

# -----------------------------
# 3. Train quantile models
# -----------------------------
quantiles = [0.1, 0.5, 0.9]
models = {}
predictions = {}

for alpha in quantiles:
    model = lgb.LGBMRegressor(
        objective="quantile",
        alpha=alpha,
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        verbosity=-1,
    )

    model.fit(X_train, y_train)

    pred = model.predict(X_test)

    models[alpha] = model
    predictions[alpha] = pred

    print(f"Quantile {alpha:.1f} model trained.")

# -----------------------------
# 4. Evaluate median prediction
# -----------------------------
median_predictions = predictions[0.5]
mae = mean_absolute_error(y_test, median_predictions)

print("\n===== LIGHTGBM QUANTILE RESULTS =====")
print(f"Median MAE: {mae:.2f} minutes")

# Quantile loss
for alpha in quantiles:
    error = y_test - predictions[alpha]
    quantile_loss = np.mean(
        np.maximum(alpha * error, (alpha - 1) * error)
    )
    print(f"Quantile {alpha:.1f} loss: {quantile_loss:.2f}")

# Prediction interval coverage
lower = np.maximum(predictions[0.1], 0)
upper = np.maximum(predictions[0.9], 0)
print("Lower > upper:", np.sum(lower > upper))
print("Negative lower predictions:", np.sum(lower < 0))
print("Negative upper predictions:", np.sum(upper < 0))

coverage = np.mean((y_test >= lower) & (y_test <= upper))
average_width = np.mean(upper - lower)

print(f"80% interval coverage: {coverage:.2%}")
print(f"Average interval width: {average_width:.2f} minutes")

# -----------------------------
# 5. Log results to MLflow
# -----------------------------
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

with mlflow.start_run(run_name="lightgbm_quantile"):
    mlflow.log_params({
        "model_type": "LightGBM quantile regression",
        "quantiles": "0.1,0.5,0.9",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "train_test_split": "80/20 chronological",
    })

    mlflow.log_metrics({
        "median_mae_minutes": float(mae),
        "interval_coverage": float(coverage),
        "average_interval_width_minutes": float(average_width),
    })

    for alpha in quantiles:
        error = y_test - predictions[alpha]
        loss = np.mean(
            np.maximum(alpha * error, (alpha - 1) * error)
        )
        mlflow.log_metric(f"quantile_{alpha:.1f}_loss", float(loss))

print("Results logged to MLflow.")

spark.stop()
