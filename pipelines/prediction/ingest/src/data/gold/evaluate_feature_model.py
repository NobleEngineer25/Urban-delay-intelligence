
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    abs as spark_abs,
    row_number,
    when,
    lit,
)
from pyspark.sql.window import Window

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

builder = (
    SparkSession.builder
    .appName("EvaluateUrbanDelayFeatureModel")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

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

# Chronological 80/20 train-test split.
ordering = Window.orderBy("trip_start_timestamp", "trip_id")

df = df.withColumn("_row_num", row_number().over(ordering))

total_rows = df.count()
split_row = int(total_rows * 0.8)

train = df.filter(col("_row_num") <= split_row).drop("_row_num")
test = df.filter(col("_row_num") > split_row).drop("_row_num")

print("Random forest:")
print("Total rows:", total_rows)
print("Train rows:", train.count())
print("Test rows:", test.count())

print("Test trip ID range:")
test.selectExpr(
    "min(trip_id) as min_trip_id",
    "max(trip_id) as max_trip_id",
).show(truncate=False)

test.selectExpr(
    "min(trip_start_timestamp) as test_start",
    "max(trip_start_timestamp) as test_end",
).show(truncate=False)

# Convert area IDs into categorical features.
categorical_cols = [
    "pickup_community_area",
    "dropoff_community_area",
]

indexers = [
    StringIndexer(
        inputCol=c,
        outputCol=f"{c}_index",
        handleInvalid="keep",
    )
    for c in categorical_cols
]

encoder = OneHotEncoder(
    inputCols=[f"{c}_index" for c in categorical_cols],
    outputCols=[f"{c}_ohe" for c in categorical_cols],
    handleInvalid="keep",
)

assembler = VectorAssembler(
inputCols=[
    "pickup_community_area_ohe",
    "dropoff_community_area_ohe",
    "hour",
    "day_of_week",
    "is_weekend",
    "month",
    "is_rush_hour",
],
    outputCol="features",
    handleInvalid="keep",
)

model = RandomForestRegressor(
    featuresCol="features",
    labelCol="trip_duration_minutes",
    predictionCol="prediction",
    numTrees=60,
    maxDepth=12,
    seed=42,
)

pipeline = Pipeline(
    stages=indexers + [encoder, assembler, model]
)

# Fit preprocessing and model using training data only.
fitted_pipeline = pipeline.fit(train)
predictions = fitted_pipeline.transform(test)

# Inspect feature importance from the trained Random Forest.
rf_model = fitted_pipeline.stages[-1]

rf_model = fitted_pipeline.stages[-1]

# Get feature names from the fitted VectorAssembler input metadata.
assembler_model = fitted_pipeline.stages[-2]
feature_names = predictions.schema["features"].metadata["ml_attr"]["attrs"]

all_features = []
for attribute_type in feature_names.values():
    all_features.extend(attribute_type)

all_features = sorted(all_features, key=lambda x: x["idx"])

importances = rf_model.featureImportances.toArray()

importance_rows = sorted(
    zip(
        [feature["name"] for feature in all_features],
        importances,
    ),
    key=lambda item: item[1],
    reverse=True,
)

print("\n===== TOP 15 FEATURE IMPORTANCES =====")
for name, importance in importance_rows[:15]:
    print(f"{name}: {importance:.4f}")
    

evaluator = RegressionEvaluator(
    labelCol="trip_duration_minutes",
    predictionCol="prediction",
    metricName="mae",
)

mae = evaluator.evaluate(predictions)


# Historical baseline using training data only
historical_baseline = (
    train.groupBy(
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
    )
    .agg(
        avg("trip_duration_minutes").alias("baseline_prediction")
    )
)

# Match historical averages to the same test trips
baseline_predictions = predictions.join(
    historical_baseline,
    on=[
        "pickup_community_area",
        "dropoff_community_area",
        "hour",
    ],
    how="left",
)

# Fall back to the training mean for unseen combinations
training_mean = train.agg(
    avg("trip_duration_minutes").alias("mean_duration")
).first()["mean_duration"]

baseline_predictions = baseline_predictions.withColumn(
    "baseline_prediction",
    when(
        col("baseline_prediction").isNull(),
        lit(training_mean),
    ).otherwise(col("baseline_prediction")),
)

# Calculate historical baseline MAE
baseline_mae = baseline_predictions.select(
    avg(
        spark_abs(
            col("trip_duration_minutes") - col("baseline_prediction")
        )
    ).alias("mae")
).first()["mae"]

print("\n===== FEATURE MODEL EVALUATION =====")
print(f"Total valid trips: {total_rows}")
print(f"Training trips: {train.count()}")
print(f"Test trips: {test.count()}")
print(f"Historical baseline MAE: {baseline_mae:.2f} minutes")
print(f"Random Forest MAE: {mae:.2f} minutes")

if mae < baseline_mae:
    print(f"MAE improvement: {baseline_mae - mae:.2f} minutes")
else:
    print(f"MAE difference: {mae - baseline_mae:.2f} minutes worse than baseline")

print("\nExample predictions:")
predictions.select(
    "trip_start_timestamp",
    "pickup_community_area",
    "dropoff_community_area",
    "trip_duration_minutes",
    "prediction",
).show(10, truncate=False)

spark.stop()
