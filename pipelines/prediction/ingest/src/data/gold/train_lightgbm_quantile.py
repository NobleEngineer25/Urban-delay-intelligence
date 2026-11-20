
import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
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

# New Gold features
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

categorical_columns = [
    "pickup_community_area",
    "dropoff_community_area",
]

train = train.dropna(
    subset=feature_columns + ["trip_duration_minutes"]
)
test = test.dropna(
    subset=feature_columns + ["trip_duration_minutes"]
)

X_train = train.select(feature_columns).toPandas()
y_train = (
    train.select("trip_duration_minutes")
    .toPandas()
    .values.ravel()
)

X_test = test.select(feature_columns).toPandas()
y_test = (
    test.select("trip_duration_minutes")
    .toPandas()
    .values.ravel()
)

# Convert community areas to categorical features.
# Use the training categories for both datasets.
for column in categorical_columns:
    X_train[column] = X_train[column].astype("category")

    unknown_categories = (
        ~X_test[column].isin(X_train[column].cat.categories)
    ).sum()

    print(
        f"Unseen test categories in {column}: "
        f"{unknown_categories}"
    )

    X_test[column] = pd.Categorical(
        X_test[column],
        categories=X_train[column].cat.categories,
    )

print("Training rows:", len(X_train))
print("Test rows:", len(X_test))
print("Features:", feature_columns)


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
# 4. Evaluate predictions
# -----------------------------
median_predictions = predictions[0.5]
mae = mean_absolute_error(y_test, median_predictions)

print("\n===== LIGHTGBM QUANTILE RESULTS =====")
print(f"Median MAE: {mae:.2f} minutes")

# Quantile loss
quantile_losses = {}

for alpha in quantiles:
    error = y_test - predictions[alpha]

    quantile_loss = np.mean(
        np.maximum(alpha * error, (alpha - 1) * error)
    )

    quantile_losses[alpha] = quantile_loss

    print(
        f"Quantile {alpha:.1f} loss: "
        f"{quantile_loss:.2f}"
    )

# Prediction interval
raw_lower = predictions[0.1]
raw_upper = predictions[0.9]

negative_lower = np.sum(raw_lower < 0)
negative_upper = np.sum(raw_upper < 0)

lower = np.maximum(raw_lower, 0)
upper = np.maximum(raw_upper, 0)

crossing_count = np.sum(lower > upper)

print("Lower > upper:", crossing_count)
print("Negative lower predictions:", negative_lower)
print("Negative upper predictions:", negative_upper)

coverage = np.mean(
    (y_test >= lower) & (y_test <= upper)
)

average_width = np.mean(upper - lower)

print(f"80% interval coverage: {coverage:.2%}")
print(f"Average interval width: {average_width:.2f} minutes")


# -----------------------------
# 5. Log results to MLflow
# -----------------------------
mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

with mlflow.start_run(run_name="lightgbm_quantile_features_v2"):
    mlflow.log_params({
        "model_type": "LightGBM quantile regression",
        "quantiles": "0.1,0.5,0.9",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "train_test_split": "80/20 chronological",
        "categorical_features": ",".join(categorical_columns),
        "features": ",".join(feature_columns),
    })

    mlflow.log_metrics({
        "median_mae_minutes": float(mae),
        "interval_coverage": float(coverage),
        "average_interval_width_minutes": float(average_width),
        "crossing_count": int(crossing_count),
    })

    for alpha, loss in quantile_losses.items():
        mlflow.log_metric(
            f"quantile_{alpha:.1f}_loss",
            float(loss),
        )

print("Results logged to MLflow.")

spark.stop()
