from pyspark.sql import SparkSession

from spark_rock_jvm_python.resources import resource_path

# Create a spark session
spark = (
    SparkSession.Builder()
    .appName(name="DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()
)

# reading a dataframe
if path := resource_path("numbers.csv"):
    first_df = spark.read.format("csv").load(path)  # pyright: ignore[reportArgumentType]
else:
    first_df = None
if __name__ == "__main__":
    print(first_df)
