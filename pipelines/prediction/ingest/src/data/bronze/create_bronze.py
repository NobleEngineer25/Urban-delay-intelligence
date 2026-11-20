
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[6]

raw_path = (
    PROJECT_ROOT
    / "pipelines/prediction/ingest/src/data/raw/chicago_taxi_2023_01.csv"
)

bronze_path = (
    PROJECT_ROOT
    / "pipelines/prediction/ingest/src/data/bronze/chicago_taxi"
)

builder = (
    SparkSession.builder
    .appName("UrbanDelayBronze")
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

try:
    print("Reading raw file:", raw_path)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data not found: {raw_path}")

    df = spark.read.csv(
        str(raw_path),
        header=True,
        inferSchema=True
    )

    print("Raw rows:", df.count())

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(str(bronze_path))

    print("Bronze Delta table saved to:", bronze_path)

finally:
    spark.stop()
