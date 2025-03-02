from pyspark.sql.session import SparkSession

from spark_rock_jvm_python.config import setup_config

setup_config()e


spark = (
    SparkSession.Builder()
    .appName("spark-rock-jvm")
    .config("spark.master", "local")
    .getOrCreate()
)

del spark
