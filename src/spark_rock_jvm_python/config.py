"""
Load environment variables for Spark
"""

import logging
import os
import sys
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import Field, dataclass, field, fields
from pathlib import Path
from types import MappingProxyType
from typing import Any

import appdirs
import yaml

logger = logging.getLogger(__name__)


class MissingEnvVarsError(Exception):
    def __init__(self, missing_vars: Sequence[str]):
        super().__init__(
            f"Missing required environment variables for PySpark session: {missing_vars}. Set in environment, or in the package's config file path {CONFIG_FILE_PATH}"
        )


class ExtraVars(Warning):
    def __init__(self, extra_vars: Sequence[str]):
        super().__init__(
            f"Ignoring unrecognized config options: {extra_vars}. Options: {[f.name for f in fields(Config)]}"
        )


class AnnotatedDataClassMixin:
    @classmethod
    def annotated(cls, field_name: str) -> MappingProxyType[Any, Any]:
        if (
            (dc_fields := getattr(cls, "__dataclass_fields__", None))
            and isinstance(dc_fields, Mapping)
            and isinstance((field := dc_fields.get(field_name, None)), Field)
        ):
            return field.metadata
        return MappingProxyType({})


@dataclass
class Config(AnnotatedDataClassMixin):
    """Env vars for running pyspark (defaults are examples for MacOS installation via HomeBrew)"""

    JAVA_HOME: str = field(metadata={"example": "/opt/homebrew/opt/openjdk@17"})
    SPARK_HOME: str = field(
        metadata={"example": "/opt/homebrew/opt/apache-spark/libexe"}
    )
    PYSPARK_PYTHON: str = field(metadata={"example": "python3"})

    @classmethod
    def load(cls) -> "Config":
        return load_config()


APP_NAME = "spark-rock-jvm-python"

CONFIG_DIR = Path(appdirs.user_config_dir(APP_NAME))

CONFIG_FILE_PATH = Path(os.path.join(CONFIG_DIR, "config.yaml"))


def load_config(config_file_path: Path = CONFIG_FILE_PATH) -> Config:
    """
    First checks if the configuration file path exists; if yes, it loads the environment variables from that file.
    Else, it checks the current environment for what's been declared.
    If any variables are missing, it raises a MissingSecretsError.
    """
    config = {}
    if config_file_path.exists():
        logger.debug(f"Loading env variables from config file: {config_file_path}")
        with open(config_file_path) as file:
            user_config: dict[str, str] = yaml.safe_load(file)
            if user_config:
                fields_keep: dict[str, str] = {}
                extra_fields: dict[str, str] = {}
                for k, v in user_config.items():
                    if k not in Config.__dataclass_fields__:
                        logger.debug(f"Extra field: {k}")
                        extra_fields[k] = v
                    else:
                        fields_keep[k] = v
                if extra_fields:
                    warnings.warn(ExtraVars(list(extra_fields.keys())))

                config.update(fields_keep)
    else:
        if config_file_path == CONFIG_FILE_PATH:
            logger.info(
                f"Package config file path {config_file_path} does not exist. Create a config at this location to configure environment variables for PySpark while working with this package."
            )
        else:
            warnings.warn(
                f"Custom config file path {config_file_path} does not exist. Create a config at {CONFIG_FILE_PATH} to configure environment variables for PySpark across sessions."
            )

    missing_fields = []
    for f in fields(Config):
        if not config.get(f.name, None):
            if not (env_val := os.environ.get(f.name, None)):
                missing_fields.append(f.name)
            else:
                logger.debug(
                    f"Loading remaining var not in config file from environment: {f.name}"
                )
                config[f.name] = str(env_val)

    if any(missing_fields):
        raise MissingEnvVarsError(missing_fields)

    return Config(**config)


def update_path_and_env(config: Config) -> None:
    """Add the env vars to the system PATH"""

    ## Set JAVA_HOME and SPARK_HOME
    def update_var(var_name: str) -> None:
        os.environ[var_name] = (config_val := getattr(config, var_name))
        if f"{config_val}/bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{config_val}/bin:" + os.environ["PATH"]

    update_var("JAVA_HOME")
    update_var("SPARK_HOME")

    ## Add PySpark to Python Path
    spark_python_path = Path(config.SPARK_HOME) / "python"
    py4j_path = next((spark_python_path / "lib").glob("py4j*.zip"), None)
    if py4j_path:
        spark_python_paths = [str(spark_python_path), str(py4j_path)]
        for path in spark_python_paths:
            if path not in sys.path:
                sys.path.insert(0, path)

    ## Set Python executable for PySpark
    os.environ["PYSPARK_PYTHON"] = config.PYSPARK_PYTHON


def setup_config(config_path: Path = CONFIG_FILE_PATH):
    config = load_config(config_path)
    update_path_and_env(config)


if __name__ == "__main__":
    setup_config()
