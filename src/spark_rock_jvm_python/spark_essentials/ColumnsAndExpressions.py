"""
A Column is a Python object referring to a column in a dataframe. It has no data inside it.
A Column is used inside Expressions to obtain data. A Column is actually the simplest type of Expression.

A "projection" is a selection which just takes a subset of columns from another dataframe


SELECT: Narrow Transformation (every input partition in original dataframe has exactly one corresponding output partiiton in the resulting dataframe)

"""

from pyspark.sql.column import Column
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.session import SparkSession

from spark_rock_jvm_python.resources.utils import resource_path


def main() -> None:
    spark = (
        SparkSession.Builder()
        .appName("DF Columns and Expressions")
        .config("spark.master", "local")
        .getOrCreate()
    )
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


if __name__ == "__main__":
    main()
