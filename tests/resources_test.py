import os
from _io import BytesIO
from typing import get_args

import pytest

from spark_rock_jvm_python.resources.utils import (
    LoadErrorType,
    ResourceFile,
    get_data_dir,
    list_expected_resource_files,
    load_files,
    resource_path,
)


def test_get_data_dir() -> None:
    data_dir = get_data_dir()
    assert data_dir.is_dir()
    assert not set(os.path.basename(d) for d in data_dir.iterdir()).difference({
        "bands.json",
        "population.json",
        "guitars.json",
        "movies.json",
        "guitarPlayers.json",
        "cars.json",
        "more_cars.json",
        "sample_text.txt",
        "yellow_taxi_jan_25_2018",
        "numbers.csv",
        "sampleTextFile.txt",
        "cars_dates.json",
        "taxi_zones.csv",
        "stocks.csv",
    })


def test_expected_resource_files():
    expected_set = {os.path.basename(str(f)) for f in list_expected_resource_files()}
    given_set = set(os.path.basename(str(f)) for f in get_data_dir().iterdir()).union(
        os.path.basename(str(f))
        for f in (get_data_dir() / "yellow_taxi_jan_25_2018").iterdir()
    )
    assert not expected_set.difference(given_set)


def test_load_files():
    result: tuple[dict[str, BytesIO], dict[str, LoadErrorType]] = load_files()
    assert not result[1]


@pytest.mark.parametrize(
    "resource_name", (resource for resource in get_args(ResourceFile))
)
def test_resource_path(resource_name) -> None:
    path = resource_path(resource_name)
