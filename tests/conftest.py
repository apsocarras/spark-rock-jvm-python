import json
import os
from dataclasses import asdict, fields
from pathlib import Path

import pytest

from spark_rock_jvm_python.config import (
    Config,
)

from . import logging_config


@pytest.fixture
def project_root() -> Path:
    cur_file = Path(__file__)
    project_root = cur_file.parents[1]
    assert os.path.basename(project_root) == "spark-rock-jvm-python"
    return project_root


@pytest.fixture
def default_config() -> Config:
    return Config(**{f.name: f.metadata.get("example", "") for f in fields(Config)})


@pytest.fixture
def madeup_config() -> Config:
    return Config(
        JAVA_HOME="foo_openjdk@7",
        SPARK_HOME="apache-spark/bar_exe",
        PYSPARK_PYTHON="python2_whoops",
        JDBC_JAR="made_up_jar_file",
    )


@pytest.fixture
def blank_env(monkeypatch):
    for field in fields(class_or_instance=Config):
        if os.environ.get(field.name):
            monkeypatch.delenv(field.name)
    yield


@pytest.fixture
def env_with_madeup_vars(monkeypatch, madeup_config):
    for f in fields(madeup_config):
        monkeypatch.setenv(f.name, getattr(madeup_config, f.name))
    yield


@pytest.fixture
def config_file_w_extra_vars(tmp_path, default_config) -> Path:
    config_dict_w_extra = asdict(default_config)
    config_dict_w_extra["extra_key"] = "extra_value"
    with open(path := (tmp_path / "config_file_w_extra.yaml"), "w") as file:
        json.dump(config_dict_w_extra, file)
    return path


@pytest.fixture
def madeup_config_path(tmp_path, madeup_config) -> Path:
    with open(path := tmp_path / "madeup_config.yaml", "w") as file:
        json.dump(asdict(madeup_config), file)
    return path


@pytest.fixture
def default_config_path(tmp_path, default_config) -> Path:
    with open(path := tmp_path / "default_config.yaml", "w") as file:
        json.dump(asdict(default_config), file)
    return path


logging_config
