import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from spark_rock_jvm_python.config import Config, update_path_and_env

logger = logging.getLogger(__name__)


def test_update_path_and_env(default_config: Config) -> None:
    """Test that the env and python path are changed as intended"""

    ## Check that env vars are set
    update_path_and_env(default_config)

    assert os.environ.get("JAVA_HOME") == default_config.JAVA_HOME
    assert os.environ.get("SPARK_HOME") == default_config.SPARK_HOME
    assert os.environ.get("PYSPARK_PYTHON") == default_config.PYSPARK_PYTHON

    ## Check that values were added to the pythonpath
    spark_python_path = Path(default_config.SPARK_HOME) / "python"
    py4j_path = next((spark_python_path / "lib").glob("py4j*"), None)

    logger.debug("SPARK_HOME/python:" + str(sys.path.index(str(spark_python_path))))
    logger.debug("PY4J:" + str(sys.path.index(str(py4j_path))))


def test_run_spark_session(default_config: Config) -> None:
    """Test that these changes actually enable running pyspark"""
    update_path_and_env(default_config)

    ## Check that a spark session can be started based on these changes
    _: SparkSession = (
        SparkSession.Builder()
        .appName("update_path_and_env_test")
        .config("spark.master", "local")
        .getOrCreate()
    )
