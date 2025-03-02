"""
Load environment variables for Spark
"""

import logging
import os
import sys
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import Field, asdict, dataclass, field, fields
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
        metadata={"example": "/opt/homebrew/opt/apache-spark/libexec"}
    )
    PYSPARK_PYTHON: str = field(metadata={"example": "python3"})
    JDBC_JAR: str = field(
        metadata={
            "example": "~/Downloads/postgresql-42.7.5.jar",
            "description": "Path to .jar for PostGres JDBC Driver.",
        },
    )

    @classmethod
    def load(cls) -> "Config":
        return load_config()


APP_NAME = "spark-rock-jvm-python"

CONFIG_DIR = Path(appdirs.user_config_dir(APP_NAME))

CONFIG_FILE_PATH = Path(os.path.join(CONFIG_DIR, "config.yaml"))


def _load_env_from_config(
    config_dict: dict[str, str], config_file_path: Path
) -> dict[str, str]:
    with open(file=config_file_path) as file:
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
                warnings.warn(ExtraVars(list(extra_fields.keys())), stacklevel=1)

            config_dict.update(fields_keep)
    return config_dict


def load_config(config_file_path: Path = CONFIG_FILE_PATH) -> Config:
    """
    First checks if the configuration file path exists; if yes, it loads the environment variables from that file.
    Else, it checks the current environment for what's been declared.
    If any variables are missing, it raises a MissingSecretsError.
    """
    config = {}
    if config_file_path.exists():
        logger.debug(f"Loading env variables from config file: {config_file_path}")
        config = _load_env_from_config(
            config_dict=config, config_file_path=config_file_path
        )
    else:
        if config_file_path == CONFIG_FILE_PATH:
            logger.info(
                f"Package config file path {config_file_path} does not exist. Create a config at this location to configure environment variables for PySpark while working with this package."
            )
        else:
            warnings.warn(
                f"Custom config file path {config_file_path} does not exist. Create a config at {CONFIG_FILE_PATH} to configure environment variables for PySpark across sessions.",
                stacklevel=1,
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

    os.environ["PYSPARK_PYTHON"] = config.PYSPARK_PYTHON
    os.environ["JDBC_JAR"] = config.JDBC_JAR

    ## Set JAVA_HOME and SPARK_HOME
    def update_var(var_name: str) -> None:
        os.environ[var_name] = (config_val := getattr(config, var_name))
        if f"{config_val}/bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{config_val}/bin:" + os.environ["PATH"]

    update_var("JAVA_HOME")
    update_var("SPARK_HOME")

    ## Add PySpark to Python Path
    spark_python_path = Path(config.SPARK_HOME) / "python"
    py4j_path = next((spark_python_path / "lib").glob("py4j*"), None)
    logger.debug(spark_python_path)
    logger.debug(py4j_path)
    if spark_python_path not in sys.path:
        sys.path.append(str(spark_python_path))
        sys.path.append(str(py4j_path))


def setup_config(config_path: Path = CONFIG_FILE_PATH):
    config = load_config(config_path)
    update_path_and_env(config)


def update_config_file(config: Config) -> None:
    if not CONFIG_DIR.exists():
        CONFIG_DIR.mkdir()
    with open(CONFIG_FILE_PATH, "w") as file:
        yaml.dump(asdict(config), file)
