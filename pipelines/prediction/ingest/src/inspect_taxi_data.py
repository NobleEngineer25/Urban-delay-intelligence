from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("InspectChicagoTaxiData")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv(
    "src/data/raw/chicago_taxi_2023_01.csv",
    header=True,
    inferSchema=True
)

print("=== SCHEMA ===")
df.printSchema()

print("\n=== FIRST 10 ROWS ===")
df.show(10, truncate=False)

print("\n=== NUMBER OF ROWS ===")
print(df.count())

print("\n=== COLUMNS ===")
print(df.columns)

spark.stop()