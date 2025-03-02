import json
import logging
import os
import random
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import pytest

from spark_rock_jvm_python.config import (
    Config,
    ExtraVars,
    MissingEnvVarsError,
    load_config,
)

logger = logging.getLogger(__name__)


def test_load_config_no_env_no_config_file(blank_env) -> None:
    with (
        patch.object(Path, "exists", return_value=False),
        pytest.raises(MissingEnvVarsError),
    ):
        _ = load_config()


def test_load_config_no_env_yes_config_file(
    blank_env, madeup_config, madeup_config_path
) -> None:
    loaded_config = load_config(madeup_config_path)
    assert loaded_config == madeup_config


def test_load_config_yes_env_no_config(env_with_madeup_vars, madeup_config) -> None:
    with (
        patch.object(Path, "exists", return_value=False),
        patch("os.path.exists", return_value=False),
    ):
        config = load_config()
        assert config == madeup_config


def test_load_config_yes_env_yes_config(
    env_with_madeup_vars,
    default_config_path,
    default_config: Config,
    monkeypatch,
    madeup_config,
    tmp_path: Path,
) -> None:
    ## Ensure that the config file values take priority over env vars
    config = load_config(default_config_path)
    assert config == default_config
    with (
        patch.object(Path, "exists", return_value=False),
        patch("os.path.exists", return_value=False),
    ):
        config = load_config()
        assert config == madeup_config

    ## Check that mix of env vars and config vars are included if config is missing some
    # Make modified copy of default config which is missing a variable
    config_dict_missing_var = asdict(default_config)
    _ = config_dict_missing_var.pop(
        missing_field := random.choice(list(config_dict_missing_var.keys()))
    )
    # Check the provided tes env does have the missing field
    assert (env_value := os.environ.get(missing_field))
    logger.debug(f"Missing var: {missing_field}")

    with open(
        missing_field_path := (tmp_path / "config_missing_field.yaml"), "w"
    ) as file:
        json.dump(config_dict_missing_var, file)

    config2 = load_config(missing_field_path)

    assert (config_value := getattr(config2, missing_field)) == env_value, (
        config_value,
        env_value,
    )

    # Check that the config dict agrees with the config file for the other values
    for k, v in config_dict_missing_var.items():
        assert getattr(config2, k) == v


def test_load_config_w_extras(config_file_w_extra_vars: Path, default_config) -> None:
    with pytest.warns(ExtraVars):
        config = load_config(config_file_w_extra_vars)
        assert config == default_config
