"""
Notes for working through the Spark Bundle at Rock the JVM (using Python)

This __init__.py sets up the environment variables for running pyspark based on a user config or the current envrionment settings.
"""

import inspect
import logging
from pathlib import Path

from spark_rock_jvm_python.config import setup_config
from spark_rock_jvm_python.resources.utils import (
    get_data_dir,
    get_writes_dir,
    make_data_dir_read_only,
    resource_path,
    write_path,
)

logger = logging.getLogger(__name__)

__data_dir = Path(str(get_data_dir()))
__write_dir = Path(str(get_writes_dir()))

setup_config()
make_data_dir_read_only()


logger.debug(f"Write-protected package data files: {__data_dir}.")
logger.info(
    f"Read package data files with {resource_path.__qualname__}: {inspect.get_annotations(resource_path)}"
)
logger.info(
    f"Folder for writing user data: {__write_dir}. Get path for writing with {write_path.__qualname__}()"
)
