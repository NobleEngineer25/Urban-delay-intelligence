"""Add pickup and dropoff H3 cells to the Silver taxi table.

Reads  pipelines/prediction/ingest/src/data/silver/chicago_taxi
Writes pipelines/prediction/ingest/src/data/silver/chicago_taxi_h3

Trips without a pickup or dropoff centroid inside Chicago cannot be indexed
and are excluded; the script prints how many and what kind of trips they are,
so the exclusion can be documented.

H3_RESOLUTION must match the disruption data in the intelligence pipeline
(pipelines/intelligence/ingest/src/geocode_h3.py, H3_RES = 8) so trips and
disruptions join on the same cells.

Run from the repo root:
    python pipelines/prediction/ingest/src/data/silver/create_silver_h3.py
"""
from pathlib import Path

import h3
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, percentile_approx, udf, when
from pyspark.sql.types import StringType

H3_RESOLUTION = 8

# Rough Chicago bounding box; same as the intelligence pipeline.
LAT_RANGE = (41.6, 42.1)
LON_RANGE = (-88.0, -87.5)

PROJECT_ROOT = Path(__file__).resolve().parents[6]
DATA_DIR = PROJECT_ROOT / "pipelines/prediction/ingest/src/data/silver"
SILVER_PATH = DATA_DIR / "chicago_taxi"
H3_PATH = DATA_DIR / "chicago_taxi_h3"


@udf(returnType=StringType())
def to_h3(lat, lon):
    if lat is None or lon is None:
        return None
    if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1] and LON_RANGE[0] <= lon <= LON_RANGE[1]):
        return None
    return h3.latlng_to_cell(float(lat), float(lon), H3_RESOLUTION)


builder = (
    SparkSession.builder
    .appName("UrbanDelaySilverH3")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    silver = spark.read.format("delta").load(str(SILVER_PATH))
    silver_count = silver.count()

    df = (
        silver
        .withColumn("pickup_h3", to_h3(col("pickup_centroid_latitude"), col("pickup_centroid_longitude")))
        .withColumn("dropoff_h3", to_h3(col("dropoff_centroid_latitude"), col("dropoff_centroid_longitude")))
        .withColumn(
            "h3_exclusion_reason",
            when(col("pickup_h3").isNull() & col("dropoff_h3").isNull(), "no pickup or dropoff location")
            .when(col("pickup_h3").isNull(), "no pickup location")
            .when(col("dropoff_h3").isNull(), "no dropoff location"),
        )
        .cache()
    )

    # Who gets excluded, compared with who is kept.
    print(f"Silver rows: {silver_count:,}")
    (
        df.withColumn("group", when(col("h3_exclusion_reason").isNull(), "kept")
                      .otherwise(col("h3_exclusion_reason")))
        .groupBy("group")
        .agg(
            count("*").alias("trips"),
            percentile_approx("trip_duration_minutes", 0.5).alias("median_minutes"),
            percentile_approx("trip_miles", 0.5).alias("median_miles"),
        )
        .orderBy("group")
        .show(truncate=False)
    )

    kept = df.filter(col("h3_exclusion_reason").isNull()).drop("h3_exclusion_reason")
    kept.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(H3_PATH))

    kept_count = spark.read.format("delta").load(str(H3_PATH)).count()
    print(f"H3 rows: {kept_count:,} ({kept_count / silver_count:.2%} of Silver), resolution {H3_RESOLUTION}")
    print("Saved to:", H3_PATH)

finally:
    spark.stop()
