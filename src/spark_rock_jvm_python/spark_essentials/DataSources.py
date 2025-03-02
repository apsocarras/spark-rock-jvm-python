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

Note that dataframes are written as directories with multiple files; the number of files depends on the size of the dataframe and the partitioning settings

"""

import logging
import os

from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import col, date_format
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_rock_jvm_python.resources.utils import (
    print_write_path_contents,
    resource_path,
    write_path,
)

logger = logging.getLogger(__name__)


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

    ## Reading data w/ mode
    cars_DF_inferred_schema = (
        spark.read.format("json")
        # .schema(car_schema)
        .option("inferSchema", "true")
        .option("mode", "failFast")  # dropMalformed, permissive (default)
        .option("path", str(cars_json))
        .load()
    )
    print(f"cars_DF_inferred_schema.schema:{cars_DF_inferred_schema.schema}")
    print(f"Year column:{cars_DF_inferred_schema.select(col('Year'))}")  # string

    cars_DF_explicit_schema = (
        spark.read.format("json")
        .schema(car_schema)
        .option("mode", "failFast")  # dropMalformed, permissive (default)
        .option("path", str(cars_json))
        .load()
    )
    print(f"cars_DF_explicit_schema.schema: {cars_DF_explicit_schema.schema}")
    print(f"Year column: {cars_DF_explicit_schema.select(col('Year'))}")  # date
    print(
        "When parsing string columns to DateType, Spark assumes the standard ISO Format (YYYY-MM-DD). This column happens to be in the ISO format YYYY-MM-DD."
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
    # Note that when writing dataframes with spark, a folder is created with multiple files
    print(
        f"Written dataframe {os.path.basename(out_path)} is a directory: {out_path.is_dir()}\n"
    )
    print_write_path_contents(out_path, indent=4)

    ## Writing DataFrames with JSON Flags
    # Date Parsing
    print(
        "\nIf we had a different date format than yyyy-MM-dd, Spark would need a date format field"
    )
    out_path = write_path("cars_dupe_date_change.json")
    # fmt: off
    cars_DF_date_changed = cars_DF \
        .withColumn("Year", date_format(col("Year"),format='dd-MM-yyyy')) \
        .limit(5)
    cars_DF_date_changed.write \
        .format('json') \
        .mode('overwrite') \
        .option('path', str(out_path)) \
        .save()

    cars_DF_date_changed_load = (
        spark.read
        .format('json')
        # .option('inferSchema', 'true') \ <- this would not fail, but would read as string
        .schema(car_schema)
        .option('path',str(out_path))
        .option('mode', 'failFast')
        .load()
    )

    def wrong_date_format_should_fail():
        raise AssertionError('Uhhh?')
    try:
        cars_DF_date_changed_load.show()
        wrong_date_format_should_fail()
    except Py4JJavaError as e:
        print(f'Date Parsing Error: "{str(e)[:min(len(str(e)),400)]}..."')

    option_map = {
        'path':str(out_path),
        'mode':'failFast',
        'dateFormat':'dd-MM-yyyy', # couple with schema
        ## Specific JSON options
        'allowSingleQuotes': "true",
        'compression': 'uncompressed' # bzip2, gzip, lz4, snappy, deflate, default: uncompressed
    }
    cars_DF_date_changed_load = (
        spark.read
        .format('json')
        .schema(car_schema)
        .options(**option_map)
        .load()
    )
    cars_DF_date_changed_load.show()

    ## You can replace .format('json') and .load() with a single '.json()' call at the end


if __name__ == "__main__":
    main()
