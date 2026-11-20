
import h3
import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import KFold

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
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "is_weekend",
    "is_rush_hour",
    "pickup_dropoff_distance_km",
]

CATEGORICAL_COLUMNS = [
    "pickup_community_area",
    "dropoff_community_area",
]

# Spatial folds must not see where the held-out trips are. Community area
# identifies the location, and a held-out H3 cell shares its community area
# with training cells, so spatial folds use only location-free features.
SPATIAL_FEATURE_COLUMNS = [
    c for c in FEATURE_COLUMNS if c not in CATEGORICAL_COLUMNS
]

# Spatial folds hold out whole blocks of neighbouring cells (the H3 parent
# at this resolution, ~36 km² at res 6), not scattered single cells, so
# test trips are not surrounded by training trips from the next cell over.
SPATIAL_BLOCK_RESOLUTION = 6

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


def prepare_features(train, test, features):
    """Prepare features and preserve training categories."""
    X_train = train[features].copy()
    X_test = test[features].copy()

    for column in [c for c in CATEGORICAL_COLUMNS if c in features]:
        X_train[column] = X_train[column].astype("category")

        training_categories = X_train[column].cat.categories

        unseen_count = (
            ~X_test[column].isin(training_categories)
        ).sum()

        if unseen_count:
            print(
                f"Unseen categories in {column}: "
                f"{unseen_count}"
            )

        X_test[column] = pd.Categorical(
            X_test[column],
            categories=training_categories,
        )

    return X_train, X_test


def evaluate_fold(df, train_mask, test_mask, label, features=FEATURE_COLUMNS):
    """Train and evaluate one cross-validation fold."""
    train = df.loc[train_mask].copy()
    test = df.loc[test_mask].copy()

    if train.empty or test.empty:
        print(f"Skipping {label}: empty train or test set.")
        return None

    X_train, X_test = prepare_features(train, test, features)

    y_train = train[TARGET]
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
# 2. Create Spark session and load Gold data
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
        [
            "trip_id",
            "trip_start_timestamp",
            "pickup_h3",
            "dropoff_h3",
        ]
        + FEATURE_COLUMNS
        + [TARGET]
    )

    df_spark = (
        df_spark
        .filter(
            col("trip_start_timestamp").isNotNull()
            & col("pickup_h3").isNotNull()
            & col("dropoff_h3").isNotNull()
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


# --------------------------------------------------
# 3. Prepare Pandas data
# --------------------------------------------------
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

print(
    "Unique pickup H3 cells:",
    df["pickup_h3"].nunique(),
)


# --------------------------------------------------
# 4. Temporal cross-validation
# Expanding training window.
# Every test fold contains later observations.
# --------------------------------------------------
temporal_results = []
n_rows = len(df)
row_indices = np.arange(n_rows)

print("\n===== TEMPORAL CROSS-VALIDATION =====")

for fold in range(N_SPLITS):
    train_end = int(n_rows * (0.50 + 0.10 * fold))
    test_end = int(n_rows * (0.60 + 0.10 * fold))

    train_mask = row_indices < train_end

    test_mask = (
        (row_indices >= train_end)
        & (row_indices < test_end)
    )

    result = evaluate_fold(
        df,
        train_mask,
        test_mask,
        f"temporal_fold_{fold + 1}",
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
# 5. Spatial cross-validation using H3 blocks
# Hold out blocks of neighbouring pickup cells.
# Exclude training trips touching a held-out block
# as either pickup or dropoff.
# Train without community-area features (see config).
# --------------------------------------------------
spatial_results = []


def to_block(cell):
    """H3 parent cell used as the spatial hold-out unit."""
    res = h3.get_resolution(cell)
    return h3.cell_to_parent(cell, min(SPATIAL_BLOCK_RESOLUTION, res))


df["pickup_block"] = df["pickup_h3"].map(to_block)
df["dropoff_block"] = df["dropoff_h3"].map(to_block)

blocks = np.sort(
    df["pickup_block"].unique()
)

print(
    "Spatial blocks (H3 res "
    f"{SPATIAL_BLOCK_RESOLUTION}): {len(blocks)}"
)

if len(blocks) < N_SPLITS:
    raise ValueError(
        f"Need at least {N_SPLITS} pickup blocks "
        "for spatial cross-validation."
    )

spatial_splitter = KFold(
    n_splits=N_SPLITS,
    shuffle=True,
    random_state=RANDOM_STATE,
)

print("\n===== SPATIAL CROSS-VALIDATION (H3 blocks) =====")

for fold, (_, test_block_idx) in enumerate(
    spatial_splitter.split(blocks),
    start=1,
):
    held_out_blocks = blocks[test_block_idx]

    # Test trips start in held-out blocks.
    test_mask = df["pickup_block"].isin(held_out_blocks)

    # Training trips must not touch held-out blocks
    # as pickup or dropoff.
    involves_held_out = (
        df["pickup_block"].isin(held_out_blocks)
        | df["dropoff_block"].isin(held_out_blocks)
    )

    train_mask = ~involves_held_out

    result = evaluate_fold(
        df,
        train_mask,
        test_mask,
        f"spatial_fold_{fold}",
        features=SPATIAL_FEATURE_COLUMNS,
    )

    if result is not None:
        result["held_out_blocks"] = len(held_out_blocks)
        result["held_out_h3_cells"] = int(
            df.loc[test_mask, "pickup_h3"].nunique()
        )
        spatial_results.append(result)


# --------------------------------------------------
# 6. Summarize results
# --------------------------------------------------
temporal_df = pd.DataFrame(temporal_results)
spatial_df = pd.DataFrame(spatial_results)

print("\n===== CROSS-VALIDATION SUMMARY =====")

for name, results in [
    ("Temporal", temporal_df),
    ("Spatial H3", spatial_df),
]:
    if results.empty:
        print(f"{name}: no valid folds.")
        continue

    print(f"\n{name} CV")
    print(f"Valid folds: {len(results)}")
    print(
        f"Mean MAE: "
        f"{results['mae_minutes'].mean():.2f} minutes"
    )
    print(
        f"Std MAE: "
        f"{results['mae_minutes'].std(ddof=0):.2f} minutes"
    )
    print(
        f"Best MAE: "
        f"{results['mae_minutes'].min():.2f} minutes"
    )
    print(
        f"Worst MAE: "
        f"{results['mae_minutes'].max():.2f} minutes"
    )


# --------------------------------------------------
# 7. Save results and log to MLflow
# --------------------------------------------------
temporal_df.to_csv(
    "temporal_cv_results.csv",
    index=False,
)

spatial_df.to_csv(
    "spatial_cv_results.csv",
    index=False,
)

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("urban-delay-intelligence")

with mlflow.start_run(
    run_name="spatiotemporal_cross_validation_h3_blocks"
):
    mlflow.log_params({
        "model_type": "LightGBM median quantile regression",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "n_splits": N_SPLITS,
        "temporal_strategy": "expanding_window",
        "spatial_strategy": "held_out_pickup_h3_blocks",
        "spatial_block_resolution": SPATIAL_BLOCK_RESOLUTION,
        "spatial_leakage_prevention": (
            "exclude training trips touching held-out blocks; "
            "no community-area features in spatial folds"
        ),
        "categorical_features": ",".join(CATEGORICAL_COLUMNS),
        "features": ",".join(FEATURE_COLUMNS),
        "spatial_features": ",".join(SPATIAL_FEATURE_COLUMNS),
        "random_state": RANDOM_STATE,
    })

    for name, results in [
        ("temporal", temporal_df),
        ("spatial", spatial_df),
    ]:
        if results.empty:
            continue

        mlflow.log_metrics({
            f"{name}_cv_mean_mae": float(
                results["mae_minutes"].mean()
            ),
            f"{name}_cv_std_mae": float(
                results["mae_minutes"].std(ddof=0)
            ),
            f"{name}_cv_worst_mae": float(
                results["mae_minutes"].max()
            ),
        })

        for _, row in results.iterrows():
            mlflow.log_metric(
                f"{name}_{row['fold']}_mae",
                float(row["mae_minutes"]),
            )

        mlflow.log_artifact(
            f"{name}_cv_results.csv"
        )

print("\nResults logged to MLflow.")
print("Saved temporal_cv_results.csv")
print("Saved spatial_cv_results.csv")
