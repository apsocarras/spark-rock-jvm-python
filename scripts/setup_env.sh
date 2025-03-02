#!/bin/bash

## Prerequisites
# Install homebrew
# brew install openjdk@17
# brew install apache-spark

# Set Java variable
# Edit JAVA_HOME variable to an installation compatible with Spark
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# Add to $PATH (if not present)
if [[ ":$PATH:" != *":$JAVA_HOME/bin:"* ]]; then
    export PATH=$JAVA_HOME/bin:$PATH
fi

# Set Spark home and add to $PATH
export SPARK_HOME="/opt/homebrew/opt/apache-spark/libexec"
if [[ ":$PATH:" != *":$SPARK_HOME/bin:"* ]]; then
    export PATH=$SPARK_HOME/bin:$PATH
fi

# Add PySpark to PYTHONPATH
if [[ ":$PYTHONPATH:" != *":$SPARK_HOME/python:"* ]]; then
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/$(ls $SPARK_HOME/python/lib | grep py4j):$PYTHONPATH
fi

# Set Python executable for PySpark
export PYSPARK_PYTHON="python3"
