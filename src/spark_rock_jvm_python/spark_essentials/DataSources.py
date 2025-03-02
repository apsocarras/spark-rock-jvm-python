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

NOTE: PySpark Dates follow Java's SimpleDateFormat syntax: https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
"""

import logging
import os
from typing import Never, TypedDict

import click
from py4j.protocol import Py4JJavaError
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, date_format
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
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


CARS_SCHEMA = StructType([
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


def working_with_json(spark: SparkSession) -> None:
    global CARS_SCHEMA
    car_schema = CARS_SCHEMA
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

    def wrong_date_format_should_fail() -> Never:
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
    cars_DF_date_changed_load: DataFrame = (
        spark.read
        .format('json')
        .schema(car_schema)
        .options(**option_map)
        .load()
    )
    cars_DF_date_changed_load.show()

    ## You can replace .format('json') and .load() with a single '.json()' call at the end


def working_with_csv(spark: SparkSession) -> None:
    """
    CSV Flags: Many, but the most important:
        - "header": "true",
        - "sep":',',
        - "nullValue": "",
    """

    stocks_schema = StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("date", DateType(), nullable=False),
        StructField("price", FloatType(), nullable=False),
    ])

    stocks_csv = resource_path("stocks.csv")
    option_map = {
        "dateFormat": "MMM d yyyy",
    }
    csv_option_map: dict[str, str] = {
        "header": "true",
        "sep": ",",
        "nullValue": "",
    }
    stocks_DF = (
        spark.read.schema(stocks_schema)
        .options(**{**csv_option_map, **option_map})
        .csv(str(stocks_csv))
    )
    stocks_DF.show()


def working_with_parquet(spark: SparkSession) -> None:
    print("Read cars_DF from earlier and write to parquet")
    global CARS_SCHEMA
    cars_json = resource_path("cars.json")
    outpath = write_path("cars_DF.parquet")

    # fmt: off
    cars_DF = spark.read \
        .json(str(cars_json), schema=CARS_SCHEMA)

    cars_DF.write \
        .mode("overwrite") \
        .parquet(str(outpath))  # or just .save() <-- .parquet is default spark format
    # fmt: on

    print_write_path_contents(outpath, indent=4)


def working_with_text(spark: SparkSession) -> None:
    sample_text_txt = resource_path("sample_text.txt")

    spark.read.text(str(sample_text_txt)).show()


def working_with_remote_db(spark: SparkSession) -> None:
    print("Ensure your docker postgres container is running")
    jbdc_options = {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://localhost:5432/rtjvm",
        "user": "docker",
        "password": "docker",
        "dbtable": "public.employees",
    }
    employees_DF: DataFrame = spark.read.format("jdbc").options(**jbdc_options).load()
    employees_DF.show()


def lesson_exercises(spark: SparkSession) -> None:
    print("""Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    """)
    movies_schema = StructType([
        StructField(name="Title", dataType=StringType()),
        StructField(name="US_Gross", dataType=LongType()),
        StructField(name="Worldwide_Gross", dataType=LongType()),
        StructField(name="US_DVD_Sales", dataType=LongType()),
        StructField(name="Production_Budget", dataType=LongType()),
        StructField(name="Release_Date", dataType=DateType()),
        StructField(name="MPAA_Rating", dataType=StringType()),
        StructField(name="Running_Time_min", dataType=IntegerType()),
        StructField(name="Distributor", dataType=StringType()),
        StructField(name="Source", dataType=StringType()),
        StructField(name="Major_Genre", dataType=StringType()),
        StructField(name="Creative_Type", dataType=StringType()),
        StructField(name="Director", dataType=StringType()),
        StructField(name="Rotten_Tomatoes_Rating", dataType=IntegerType()),
        StructField(name="IMDB_Rating", dataType=FloatType()),
        StructField(name="IMDB_Votes", dataType=IntegerType()),
    ])
    movies_json = resource_path("movies.json")
    movies_DF = spark.read.json(
        str(movies_json), schema=movies_schema, dateFormat="dd MMM yy"
    )
    movies_DF.limit(2).show(vertical=True)

    # Write to tab-separated file
    movies_tab_sep_path = write_path("movies_tab_sep.tsv")
    # fmt: off
    movies_DF.write.csv(
        str(movies_tab_sep_path),
        nullValue="",
        header=True,
        sep="\t",
        mode="overwrite",
        dateFormat="dd MMM yy"
    )
    # fmt: on

    # Write to parquet
    movies_parquet_path = write_path("movies.parquet")
    movies_DF.write.parquet(path=str(movies_parquet_path), mode="overwrite")

    # Write to table "public.movies" in the Postgres DB
    class JdbcOptions(TypedDict):
        driver: str
        url: str
        user: str
        password: str
        dbtable: str

    jbdc_options: JdbcOptions = {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://localhost:5432/rtjvm",
        "user": "docker",
        "password": "docker",
        "dbtable": "public.movies",
    }

    (
        movies_DF.write.format("jdbc")
        .options(**jbdc_options)  # pyright: ignore[reportArgumentType]
        .mode("overwrite")
        .save()  # or .save(mode="overwrite")
    )

    movies_DF_from_db = (
        spark.read.format("jdbc")
        .schema(movies_schema)
        .option("dateFormat", "dd MMM yy")
        .options(**jbdc_options)  # pyright: ignore[reportArgumentType])
        .load()
    )
    movies_DF_from_db.limit(2).show(vertical=True)


@click.command(help="If no flags provided, will run all lessons")
@click.option(
    "--json", "-j", is_flag=True, default=None, help="Whether to run json lesson"
)
@click.option(
    "--csv", "-c", is_flag=True, default=None, help="Whether to run csv lesson"
)
@click.option(
    "--parquet", "-p", is_flag=True, default=None, help="Whether to run parquet lesson"
)
@click.option(
    "--text", "-t", is_flag=True, default=None, help="Whether to run text lesson"
)
@click.option(
    "--db", is_flag=True, default=None, help="Whether to run remote_database lesson"
)
@click.option(
    "--exercises",
    "-ex",
    is_flag=True,
    default=None,
    help="Whether to run lesson_exercises",
)
def main(
    json: bool | None = None,
    csv: bool | None = None,
    parquet: bool | None = None,
    text: bool | None = None,
    db: bool | None = None,
    exercises: bool | None = None,
):
    spark: SparkSession = (
        SparkSession.Builder()
        .appName("Data Sources and Formats")
        .config("spark.master", "local")
        .config("spark.jars", "/Users/alex/Downloads/postgresql-42.7.5.jar")
        .getOrCreate()
    )
    if all(x is None for x in (json, csv, parquet, text, db, exercises)):
        json = True
        csv = True
        parquet = True
        text = True
        db = True
        exercises = True

    if json:
        logger.info("Running lesson for json")
        working_with_json(spark)

    if csv:
        logger.info("Running lesson for csv")
        working_with_csv(spark)

    if parquet:
        logger.info("Running lesson for parquet")
        working_with_parquet(spark)

    if text:
        logger.info("Running lesson for text")
        working_with_text(spark)

    if db:
        logger.info("Running lesson for remote database")
        working_with_remote_db(spark)

    if exercises:
        logger.info("Running lesson exercises")
        lesson_exercises(spark)


if __name__ == "__main__":
    main()
