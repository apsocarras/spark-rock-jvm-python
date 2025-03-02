"""
Data Sources and Formats

Reading DF:
    - format
    - schema or inferSchema = true
    - zero or more options

Writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists
    - path
    - zero or more options
"""

from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_rock_jvm_python.resources.utils import resource_path, write_path


def main():
    spark = (
        SparkSession.Builder()
        .appName("Data Sources and Formats")
        .config("spark.master", "local")
        .getOrCreate()
    )

    car_schema = StructType([
        StructField("Name", StringType()),
        StructField("Miles_per_Gallon", DoubleType()),
        StructField("Cylinders", LongType()),
        StructField("Displacement", DoubleType()),
        StructField("Horsepower", LongType()),
        StructField("Weight_in_lbs", LongType()),
        StructField("Acceleration", DoubleType()),
        StructField("Year", DateType()),
        StructField("Origin", StringType()),
    ])

    cars_json = resource_path("cars.json")

    ## Reading data w/ mode=failFast
    cars_DF = (
        spark.read.format("json")
        .schema(car_schema)
        .option("mode", "failFast")  # dropMalformed, permissive (default)
        .option("path", str(cars_json))
        .load()
    )

    ## Using an option map
    option_map = {"mode": "failFast", "path": str(cars_json), "inferSchema": True}
    cars_DF = spark.read.format("json").options(**option_map).load()

    ## Writing a DataFrame
    # fmt: off
    out_path = write_path("cars_dupe.json")
    cars_DF.write \
        .format("json") \
        .mode("overwrite")\
        .option("path", str(out_path))\
        .save() # or: .save(path=str(out_path)), w/o option
    cars_DF_dupe = spark.read.format("json") \
        .option("path", str(out_path)) \
        .load()
    # fmt: on
    cars_DF_dupe.show(2)


if __name__ == "__main__":
    main()
