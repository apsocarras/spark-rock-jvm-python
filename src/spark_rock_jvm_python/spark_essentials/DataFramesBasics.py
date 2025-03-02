"""
A dataframe is:
    - A schema (column names with data types)
    - Combined with a distributed collection of rows (arrays) conforming to the schema

Spark Scala: Spark data types are explained by case objects it uses internally.
It does not know the types at compile time but at runtime when Spark evalutes the dataset.
    (in Python, these are the types in pyspark.sql.types)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_rock_jvm_python.config import setup_config
from spark_rock_jvm_python.resources.utils import resource_path


def main() -> None:
    # Create a spark session
    spark = (
        SparkSession.Builder()
        .appName(name="DataFrames Basics")
        .config("spark.master", "local")  # for cluster, this is replaced by URL
        .getOrCreate()
    )

    # reading a dataframe
    cars_json = resource_path("cars.json")
    first_df = (
        spark.read.format("json").option("inferSchema", "true").load(str(cars_json))
    )

    first_df.show(n=2, truncate=False, vertical=True)

    first_df.printSchema()

    print(first_df.take(10))

    cars_schema = StructType([
        StructField(name="Name", dataType=StringType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Miles_per_Gallon", dataType=IntegerType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Cylinders", dataType=IntegerType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Displacement", dataType=IntegerType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Horsepower", dataType=IntegerType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Weight_in_lbs", dataType=IntegerType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Acceleration", dataType=DoubleType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Year", dataType=StringType(), nullable=False),  # pyright: ignore[reportArgumentType]
        StructField(name="Origin", dataType=StringType(), nullable=False),  # pyright: ignore[reportArgumentType]
    ])

    cars_df_schema = first_df.schema

    cars_df_with_schema = (
        spark.read.format("json").schema(cars_df_schema).load(str(cars_json))
    )

    # Create rows manual (only really useful for testing)
    # my_row = Row("chevy malibu", 18, 8, 301, 130, 2504, 12.0, "1970-01-01", "USA")


if __name__ == "__main__":
    setup_config()
    main()
