"""
Notes for working through the Spark Bundle at Rock the JVM (using Python)

This __init__.py sets up the environment variables for running pyspark based on a user config or the current envrionment settings.
"""

from spark_rock_jvm_python.config import setup_config

setup_config()
