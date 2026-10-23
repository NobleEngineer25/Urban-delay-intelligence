import numpy as np
import pandas as pd
import lightgbm as lgb
import mlflow

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------
GOLD_PATH = (
    "pipelines/prediction/ingest/src/data/gold/"
    "chicago_taxi_features"
)

FEATURE_COLUMNS = [
    "pickup_community_area",
    "dropoff_community_area",
    "hour",
    "day_of_week",
    "is_weekend",
    "month",
    "is_rush_hour",
]

TARGET = "trip_duration_minutes"
N_SPLITS = 5
RANDOM_STATE = 42


def create_model():
    """Create the median LightGBM quantile model."""
    return lgb.LGBMRegressor(
        objective="quantile",
        alpha=0.5,
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        random_state=RANDOM_STATE,
        verbosity=-1,
    )


def evaluate_fold(df, train_mask, test_mask, label):
    """Train and evaluate one fold."""
    train = df.loc[train_mask]
    test = df.loc[test_mask]

    if train.empty or test.empty:
        print(f"Skipping {label}: empty train or test set.")
        return None

    X_train = train[FEATURE_COLUMNS]
    y_train = train[TARGET]
    X_test = test[FEATURE_COLUMNS]
    y_test = test[TARGET]

    model = create_model()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)

    result = {
        "fold": label,
        "train_rows": len(train),
        "test_rows": len(test),
        "mae_minutes": float(mae),
    }

    print(
        f"{label}: train={len(train):,}, "
        f"test={len(test):,}, MAE={mae:.2f} minutes"
    )

    return result


# --------------------------------------------------
# 2. Load and prepare Gold data
# --------------------------------------------------
builder = (
    SparkSession.builder
    .appName("UrbanDelaySpatiotemporalCV")
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
    df_spark = spark.read.format("delta").load(GOLD_PATH)

    required_columns = (
        ["trip_id", "trip_start_timestamp"]
        + FEATURE_COLUMNS
        + [TARGET]
    )

    df_spark = (
        df_spark
        .filter(
            col("trip_start_timestamp").isNotNull()
            & col("pickup_community_area").isNotNull()
            & col("dropoff_community_area").isNotNull()
            & col(TARGET).isNotNull()
            & (col(TARGET) > 0)
        )
        .select(*required_columns)
        .dropna(subset=FEATURE_COLUMNS + [TARGET])
    )

    df = df_spark.toPandas()

finally:
    spark.stop()

df["trip_start_timestamp"] = pd.to_datetime(
    df["trip_start_timestamp"]
)

df = (
    df.sort_values(
        ["trip_start_timestamp", "trip_id"]
    )
    .reset_index(drop=True)
)

print(f"\nTotal usable rows: {len(df):,}")
print(
    "Date range:",
    df["trip_start_timestamp"].min(),
    "to",
    df["trip_start_timestamp"].max(),
)


# --------------------------------------------------
# 3. Temporal cross-validation
# Expanding training window; each test fold is later
# than its corresponding training data.
# --------------------------------------------------
temporal_results = []
n_rows = len(df)

print("\n===== TEMPORAL CROSS-VALIDATION =====")

for fold in range(N_SPLITS):
    train_end = int(n_rows * (0.50 + 0.10 * fold))
    test_end = int(n_rows * (0.60 + 0.10 * fold))

    train_mask = np.arange(n_rows) < train_end
    test_mask = (
        (np.arange(n_rows) >= train_end)
        & (np.arange(n_rows) < test_end)
    )

    result = evaluate_fold(
        df, train_mask, test_mask, f"temporal_fold_{fold + 1}"
    )

    if result is not None:
        result["test_start"] = str(
            df.loc[test_mask, "trip_start_timestamp"].min()
        )
        result["test_end"] = str(
            df.loc[test_mask, "trip_start_timestamp"].max()
        )
        temporal_results.append(result)


# --------------------------------------------------
# 4. Spatial cross-validation
# Hold out pickup areas. Remove training trips that
# involve any held-out area as pickup or dropoff.
# --------------------------------------------------
spatial_results = []

areas = np.sort(
    df["pickup_community_area"].unique()
)

if len(areas) < N_SPLITS:
    raise ValueError(
        f"Need at least {N_SPLITS} pickup areas "
        "for spatial cross-validation."
    )

area_splitter = KFold(
    n_splits=N_SPLITS,
    shuffle=True,
    random_state=RANDOM_STATE,
)

print("\n===== SPATIAL CROSS-VALIDATION =====")

for fold, (train_area_idx, test_area_idx) in enumerate(
    area_splitter.split(areas), start=1
):
    held_out_areas = areas[test_area_idx]

    # Test trips originate in held-out pickup areas.
    test_mask = df["pickup_community_area"].isin(
        held_out_areas
    )

    # Exclude every training trip involving a held-out
    # area, whether as pickup or dropoff.
    involves_held_out = (
        df["pickup_community_area"].isin(held_out_areas)
        | df["dropoff_community_area"].isin(held_out_areas)
    )
    train_mask = ~involves_held_out

    result = evaluate_fold(
        df, train_mask, test_mask, f"spatial_fold_{fold}"
    )

    if result is not None:
        result["held_out_pickup_areas"] = ",".join(
            str(area) for area in held_out_areas
        )
        spatial_results.append(result)


# --------------------------------------------------
# 5. Summarize and log to MLflow
# --------------------------------------------------
temporal_df = pd.DataFrame(temporal_results)
spatial_df = pd.DataFrame(spatial_results)

print("\n===== CROSS-VALIDATION SUMMARY =====")

for name, results in [
    ("Temporal", temporal_df),
    ("Spatial", spatial_df),
]:
    if results.empty:
        print(f"{name}: no valid folds.")
        continue

    print(f"\n{name} CV")
    print(f"Valid folds: {len(results)}")
    print(f"Mean MAE: {results['mae_minutes'].mean():.2f} minutes")
    print(f"Std MAE: {results['mae_minutes'].std(ddof=0):.2f} minutes")
    print(f"Best MAE: {results['mae_minutes'].min():.2f} minutes")
    print(f"Worst MAE: {results['mae_minutes'].max():.2f} minutes")

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

with mlflow.start_run(run_name="spatiotemporal_cross_validation"):
    mlflow.log_params({
        "model_type": "LightGBM median quantile regression",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "n_splits": N_SPLITS,
        "temporal_strategy": "expanding_window",
        "spatial_strategy": "held_out_pickup_areas",
        "random_state": RANDOM_STATE,
    })

    for name, results in [
        ("temporal", temporal_df),
        ("spatial", spatial_df),
    ]:
        if results.empty:
            continue

        mlflow.log_metric(
            f"{name}_cv_mean_mae",
            float(results["mae_minutes"].mean()),
        )
        mlflow.log_metric(
            f"{name}_cv_std_mae",
            float(results["mae_minutes"].std(ddof=0)),
        )
        mlflow.log_metric(
            f"{name}_cv_worst_mae",
            float(results["mae_minutes"].max()),
        )

        for _, row in results.iterrows():
            mlflow.log_metric(
                f"{name}_{row['fold']}_mae",
                float(row["mae_minutes"]),
            )

    temporal_df.to_csv(
        "temporal_cv_results.csv", index=False
    )
    spatial_df.to_csv(
        "spatial_cv_results.csv", index=False
    )

    mlflow.log_artifact("temporal_cv_results.csv")
    mlflow.log_artifact("spatial_cv_results.csv")

print("\nResults logged to MLflow.")
print("Saved temporal_cv_results.csv")
print("Saved spatial_cv_results.csv")
