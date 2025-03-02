"""
A dataframe is:
    - A schema (column names with data types)
    - Combined with a distributed collection of rows (arrays) conforming to the schema
    - DataFrames have schemas, Rows do not

Spark Scala: Spark data types are explained by case objects it uses internally.
It does not know the types at compile time but at runtime when Spark evalutes the dataset.
    (in Python, these are the types in pyspark.sql.types)

Spark Scala: When creating a dataframe manually from rows (tuples), you can't specify a schema because the types *are* known at compile time.
    (Python): There is a schema parameter in spark.createDataFrame()


FloatType vs IntegerType:
    - Note that python ints are not cast to FloatType when specifying a schema (see exercises)
"""

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    DoubleType,
    FloatType,
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

    # reading a dataframe w/ inferred schema
    cars_json = resource_path("cars.json")

    first_df = (
        spark.read.format("json")
        .option("inferSchema", value="true")
        .load(str(cars_json))
    )

    first_df.show(n=2, truncate=False, vertical=True)

    first_df.printSchema()

    print(first_df.take(10))

    cars_schema = StructType([
        StructField(name="Name", dataType=StringType(), nullable=False),
        StructField(name="Miles_per_Gallon", dataType=IntegerType(), nullable=False),
        StructField(name="Cylinders", dataType=IntegerType(), nullable=False),
        StructField(name="Displacement", dataType=IntegerType(), nullable=False),
        StructField(name="Horsepower", dataType=IntegerType(), nullable=False),
        StructField(name="Weight_in_lbs", dataType=IntegerType(), nullable=False),
        StructField(name="Acceleration", dataType=DoubleType(), nullable=False),
        StructField(name="Year", dataType=StringType(), nullable=False),
        StructField(name="Origin", dataType=StringType(), nullable=False),
    ])

    cars_df_schema = first_df.schema

    ## reading a dataframe with explicit schema
    cars_df_with_schema = (
        spark.read.format("json").schema(cars_df_schema).load(str(cars_json))
    )

    # Create rows manual (only really useful for testing)
    # my_row = Row("chevy malibu", 18, 8, 301, 130, 2504, 12.0, "1970-01-01", "USA")

    # Create DF from rows
    car_rows = (
        ("chevrolet chevelle malibu", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
        ("buick skylark 320", 15, 8, 350, 165, 3693, 11.5, "1970-01-01", "USA"),
        ("plymouth satellite", 18, 8, 318, 150, 3436, 11.0, "1970-01-01", "USA"),
        ("amc rebel sst", 16, 8, 304, 150, 3433, 12.0, "1970-01-01", "USA"),
        ("ford torino", 17, 8, 302, 140, 3449, 10.5, "1970-01-01", "USA"),
        ("ford galaxie 500", 15, 8, 429, 198, 4341, 10.0, "1970-01-01", "USA"),
        ("chevrolet impala", 14, 8, 454, 220, 4354, 9.0, "1970-01-01", "USA"),
        ("plymouth fury iii", 14, 8, 440, 215, 4312, 8.5, "1970-01-01", "USA"),
        ("pontiac catalina", 14, 8, 455, 225, 4425, 10.0, "1970-01-01", "USA"),
        ("amc ambassador dpl", 15, 8, 390, 190, 3850, 8.5, "1970-01-01", "USA"),
    )

    car_rows_manual_df = spark.createDataFrame(car_rows, schema=cars_schema)

    ## EXERCISES ##
    # Exercise:
    # 1) Create a manual DF describing smartphones
    #   - make
    #   - model
    #   - screen dimension
    #   - camera megapixels
    #
    # 2) Read another file from the data/ folder, e.g. movies.json
    #   - print its schema
    #   - count the number of rows, call count()
    #
    smartphone_df_schema = StructType([
        StructField(name="make", dataType=StringType()),
        StructField(name="model", dataType=StringType()),
        StructField(name="screen_dimension", dataType=FloatType()),
        StructField(name="camera_megapixels", dataType=FloatType()),
    ])
    smartphone_df_data = (
        ("apple", "iphone 13", 703.5, 12.8),
        ("google", "pixel 6", 696.8, 15.2),
        ("microsoft", "windows phone 1", 620.2, 7.6),
        # ("microsoft", "windows phone 1", 620, 7.6) <-- Would cause a type error
    )
    smartphones_df_manual: DataFrame = spark.createDataFrame(
        smartphone_df_data, schema=smartphone_df_schema
    )

    smartphones_df_manual.show()

    movies_json = resource_path("movies.json")

    # fmt: off
    movies_df = (
        spark.read.format("json")
        .option("inferSchema", "true")
        .load(str(movies_json))
    )
    # fmt: on
    print(movies_df.count())


if __name__ == "__main__":
    setup_config()
    main()
