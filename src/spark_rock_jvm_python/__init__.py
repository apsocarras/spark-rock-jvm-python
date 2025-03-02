"""
Notes for working through the Spark Bundle at Rock the JVM (using Python)
This __init__.py initializes the environment variables needed for running pyspark locally.
"""

import os
import subprocess
import sys

from spark_rock_jvm_python.config import setup_config

setup_config()
