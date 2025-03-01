"""
A dataframe is:
    - A schema (column names with data types)
    - Combined with a distributed collection of rows (arrays) conforming to the schema

Spark data types are explained by case objects it uses internally.
It does not know the types at compile time but at runtime when Spark evalutes the dataset.

e.g.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

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

    longType = LongType


if __name__ == "__main__":
    main()
