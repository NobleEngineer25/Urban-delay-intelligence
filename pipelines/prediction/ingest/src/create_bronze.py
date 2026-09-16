from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


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


# Read raw CSV
df = spark.read.csv(
    "src/data/raw/chicago_taxi_2023_01.csv",
    header=True,
    inferSchema=True
)

print("Raw rows:", df.count())


# Save as Delta Bronze table
bronze_path = "data/bronze/chicago_taxi"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)


print("Bronze Delta table saved to:", bronze_path)

spark.stop()