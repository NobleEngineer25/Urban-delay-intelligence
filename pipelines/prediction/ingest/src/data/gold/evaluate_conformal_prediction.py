
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

import lightgbm as lgb
import mlflow
import numpy as np

from sklearn.metrics import mean_absolute_error

from conformal_prediction import ConformalPredictionWrapper


# 1. Spark session
builder = (
    SparkSession.builder
    .appName("UrbanDelayConformalPrediction")
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

try:
    gold_path = (
        "pipelines/prediction/ingest/src/data/gold/"
        "chicago_taxi_features"
    )

    df = spark.read.format("delta").load(gold_path)

    df = df.filter(
        col("trip_start_timestamp").isNotNull()
        & col("pickup_community_area").isNotNull()
        & col("dropoff_community_area").isNotNull()
        & col("trip_duration_minutes").isNotNull()
        & (col("trip_duration_minutes") > 0)
    )

    ordering = Window.orderBy("trip_start_timestamp", "trip_id")
    df = df.withColumn("_row_num", row_number().over(ordering))

    total_rows = df.count()
    train_end = int(total_rows * 0.64)
    calibration_end = int(total_rows * 0.80)

    train = df.filter(col("_row_num") <= train_end)
    calibration = df.filter(
        (col("_row_num") > train_end)
        & (col("_row_num") <= calibration_end)
    )
    test = df.filter(col("_row_num") > calibration_end)

    feature_columns = [
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
        "day_of_week",
        "is_weekend",
        "month",
        "is_rush_hour",
    ]

    required_columns = feature_columns + ["trip_duration_minutes"]

    train = train.dropna(subset=required_columns)
    calibration = calibration.dropna(subset=required_columns)
    test = test.dropna(subset=required_columns)

    def to_arrays(frame):
        X = frame.select(feature_columns).toPandas()
        y = (
            frame.select("trip_duration_minutes")
            .toPandas()
            .values.ravel()
        )
        return X, y

    X_train, y_train = to_arrays(train)
    X_cal, y_cal = to_arrays(calibration)
    X_test, y_test = to_arrays(test)

    print("Training rows:", len(X_train))
    print("Calibration rows:", len(X_cal))
    print("Test rows:", len(X_test))

    # 2. Train lower and upper quantile models
    models = {}

    for alpha in [0.1, 0.9]:
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
        models[alpha] = model

        print(f"Quantile {alpha:.1f} model trained.")

    # 3. Calibrate the prediction interval
    wrapper = ConformalPredictionWrapper(
        lower_model=models[0.1],
        upper_model=models[0.9],
        coverage=0.80,
    )

    wrapper.calibrate(X_cal, y_cal)
    lower, upper = wrapper.predict_interval(X_test)

    # 4. Evaluate on untouched test data
    median_model = lgb.LGBMRegressor(
        objective="quantile",
        alpha=0.5,
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        verbosity=-1,
    )
    median_model.fit(X_train, y_train)
    median_predictions = median_model.predict(X_test)

    mae = mean_absolute_error(y_test, median_predictions)
    coverage = np.mean((y_test >= lower) & (y_test <= upper))
    average_width = np.mean(upper - lower)

    print("\n===== CONFORMAL PREDICTION RESULTS =====")
    print(f"Median MAE: {mae:.2f} minutes")
    print(f"Target coverage: {wrapper.coverage:.0%}")
    print(f"Actual test coverage: {coverage:.2%}")
    print(f"Average interval width: {average_width:.2f} minutes")
    print(f"Conformal adjustment: {wrapper.adjustment_:.2f} minutes")

    # 5. Log metrics to MLflow
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("urban-delay-intelligence")

    with mlflow.start_run(run_name="lightgbm_conformal_prediction"):
        mlflow.log_params({
            "model_type": "LightGBM CQR",
            "quantiles": "0.1,0.5,0.9",
            "target_coverage": 0.80,
            "train_fraction": 0.64,
            "calibration_fraction": 0.16,
            "test_fraction": 0.20,
            "n_estimators": 300,
            "learning_rate": 0.05,
            "num_leaves": 31,
        })

        mlflow.log_metrics({
            "median_mae_minutes": float(mae),
            "target_coverage": 0.80,
            "actual_test_coverage": float(coverage),
            "average_interval_width_minutes": float(average_width),
            "conformal_adjustment_minutes": float(wrapper.adjustment_),
        })

    print("Results logged to MLflow.")

finally:
    spark.stop()
