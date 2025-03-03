"""
A Column is a Python object referring to a column in a dataframe. It has no data inside it.
A Column is used inside Expressions to obtain data. A Column is actually the simplest type of Expression.

A "projection" is a selection which just takes a subset of columns from another dataframe


SELECT: Narrow Transformation (every input partition in original dataframe has exactly one corresponding output partiiton in the resulting dataframe)

"""

from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_rock_jvm_python.resources.utils import resource_path

spark: SparkSession = (
    SparkSession.Builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()
)


def main(spark: SparkSession) -> None:
    cars_df_json = resource_path("cars.json")
    # fmt: off
    carsDF = (
        spark.read
        .option('inferSchema', 'true')
        .json(str(cars_df_json))
    )
    carsDF.limit(2).show()
    # fmt: on
    print("Creating new columns:")
    _: Column = carsDF.colRegex("Name")
    name_column: Column = carsDF.Name

    carsDF.select(
        name_column
    )  # new DF with just a single column # pyright: ignore[reportAssignmentType]

    print(
        "\nColumn selection methods. Can't mix plain strings with other methods in the same select() call."
    )
    cars_df_select_methods = carsDF.select(
        name_column,
        col("Acceleration"),
        expr("Origin"),  # EXPRESSION
        col("*"),
    ).select("Name", "Acceleration", "Origin", "Miles_per_Gallon", "Displacement")

    cars_df_select_methods.limit(2).show()

    print("\nColumn Expressions. ")
    weight_in_kg_expression = carsDF.Weight_in_lbs / 2.2

    print(
        "\nexpr() operator has a different implementation of certain operations like division so you may see slightly different results"
    )
    cars_with_weights_df = carsDF.select(
        col("Name"),
        col("Weight_in_lbs"),
        weight_in_kg_expression.alias("Weight_in_kg"),
        expr("Weight_in_lbs / 2.2").alias("Weight_in_kg_2"),
    )
    cars_with_weights_df.limit(2).show()

    print("\nselectExpr")
    carsDF.selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2.2 AS Weight_in_lbs_2")

    print("\nProcessing DataFrames")

    print("\nAdd a Column")
    cars_with_kg3 = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
    cars_with_kg3.printSchema()

    print("\nRename a Column")
    carsDF_with_renamed_lbs_DF = cars_with_kg3.withColumnRenamed(
        "Weight_in_lbs", "Weight in pounds"
    )
    cars_with_kg3.printSchema()
    print(
        "WARNING: If adding a column with spaces, escape the name of the column with `` in expressions or the expression parser won't know when the expression ends."
    )
    carsDF_with_renamed_lbs_DF.selectExpr("`Weight in pounds`").show()
    print("\n Drop a column")
    cars_with_kg3.drop("Cylinders", "Displacement").printSchema()

    print("\nFiltering DataFrames (.filter() == .where())")

    carsDF.filter(col("Origin") == lit("Europe")).limit(5).show()
    carsDF.where(col("Origin") == lit("Europe")).limit(5).show()
    expr_str = "Origin = 'USA'"

    print(f"\nFiltering with Expression Strings: {expr_str}")

    carsDF.filter("Origin = 'USA'")

    print('\nChain Filters with "&", or a SQL selectExpr')

    carsDF.filter((col("Origin") == lit("USA")) & (col("Horsepower") > 150)).select(
        "Origin", "Horsepower"
    ).show()

    carsDF.filter('Origin = "USA" and "Horsepower" > 150').select(
        "Origin", "Horsepower"
    ).show()

    print("\nAdding more rows: .union()")
    more_cars_json = resource_path("more_cars.json")
    more_cars_df = spark.read.option("inferSchema", "true").json(str(more_cars_json))
    all_cars_df = carsDF.union(more_cars_df)

    print("\n Distinct Values: distinct()")
    all_countries_df = carsDF.select("Origin").distinct()
    all_countries_df.show()


def exercises(spark: SparkSession):
    """
    * Exercises
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    * Use as many versions as possible
    """

    movies_json = resource_path("movies.json")
    movie_date_format = "dd-MMM-yy"
    # Read #1: inferred schema
    movies_DF = spark.read.json(
        path=str(movies_json), dateFormat=movie_date_format, mode="failFast"
    )
    movies_DF = (
        spark.read.format("json")
        .option("inferSchema", True)
        .option("mode", "failFast")
        .option("dateFormat", movie_date_format)
        .option("path", str(movies_json))
        .load()
    )
    option_map: dict[str, bool | str] = {
        "inferSchema": True,
        "mode": "failFast",
        "dateFormat": movie_date_format,
        "path": str(movies_json),
    }
    movies_DF = spark.read.format("json").options(**option_map).load()

    # Read #2: explicit schema
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
    movies_DF = spark.read.json(
        path=str(movies_json),
        schema=movies_schema,
        dateFormat=movie_date_format,
        mode="failFast",
    )
    movies_DF = (
        spark.read.format("json")
        .schema(movies_schema)
        .option("mode", "failFast")
        .option("dateFormat", movie_date_format)
        .option("path", str(movies_json))
        .load()
    )

    # 2. Select Two columns of your choice
    print(movies_DF.select("Title", "US_Gross").take(1))
    print(movies_DF.select(col("Title"), col("US_Gross")).take(1))
    print(movies_DF.selectExpr("Title", "US_Gross").take(1))
    print(movies_DF.select(movies_DF.Title, movies_DF.US_Gross).take(1))
    print(movies_DF.select(movies_DF["Title"], movies_DF["US_Gross"]).take(1))
    print(
        movies_DF.select(
            movies_DF.colRegex("Title"), movies_DF.colRegex("US_Gross")
        ).take(1)
    )
    print(movies_DF.select(["Title", "US_Gross"]).take(1))

    # 3. Create another column summing total profit: US_Gross + Worldwide_Gross + DVD sales
    total_profit = "Total_Profit"
    dvd_col_name = "US_DVD_Sales"
    ww_gross = "WorldWide_Gross"
    us_gross = "US_Gross"

    movies_with_total_profit_DF: DataFrame = movies_DF.fillna(
        cols := {
            dvd_col_name: 0,
            ww_gross: 0,
            us_gross: 0,
        }
    ).select("Title", *cols.keys())

    print(
        movies_with_total_profit_DF.withColumn(
            total_profit, col(us_gross) + col(ww_gross) + col(dvd_col_name)
        ).take(1)
    )

    total_profit_col = (col(us_gross) + col(ww_gross) + col(dvd_col_name)).alias(
        total_profit
    )
    print(movies_with_total_profit_DF.select(col("*"), total_profit_col).take(1))

    print(
        movies_with_total_profit_DF.selectExpr(
            "*", f"{us_gross} + {ww_gross} + {dvd_col_name} AS {total_profit}"
        ).take(1)
    )


if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.Builder()
        .appName("DF Columns and Expressions")
        .config("spark.master", "local")
        .getOrCreate()
    )

    main(spark)
    exercises(spark)
