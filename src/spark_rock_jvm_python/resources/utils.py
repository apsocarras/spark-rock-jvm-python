import importlib
import importlib.resources
from importlib.abc import Traversable
from io import BytesIO
from pathlib import Path
from typing import Literal

import spark_rock_jvm_python.resources.data as _data_resources


class ResourceLoadingError(Exception):
    pass


ResourceFile = Literal[
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
]

LoadErrorType = Literal["missing_file", "error_loading"]


def get_data_dir() -> Traversable:
    data_dir = importlib.resources.files(_data_resources)
    return data_dir


def resource_path(resource: ResourceFile, /) -> Path:
    data_dir = get_data_dir()
    if (traversable := data_dir / resource).is_file():
        return Path(str(traversable))
    elif resource == "yellow_taxi_jan_25_2018":
        return Path(str(data_dir / resource))
    else:
        raise FileNotFoundError(resource)


def list_expected_resource_files() -> tuple[Traversable, ...]:
    """Lists the expected source example files in the data directory."""
    data_dir = get_data_dir()
    expected_files = (
        data_dir / "bands.json",
        data_dir / "population.json",
        data_dir / "guitars.json",
        data_dir / "movies.json",
        data_dir / "guitarPlayers.json",
        data_dir / "cars.json",
        data_dir / "more_cars.json",
        data_dir / "sample_text.txt",
        data_dir / "yellow_taxi_jan_25_2018" / "_SUCCESS",
        data_dir / "yellow_taxi_jan_25_2018" / "._SUCCESS.crc",
        data_dir
        / "yellow_taxi_jan_25_2018"
        / ".part-00000-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet.crc",
        data_dir
        / "yellow_taxi_jan_25_2018"
        / ".part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet.crc",
        data_dir
        / "yellow_taxi_jan_25_2018"
        / "part-00000-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet",
        data_dir
        / "yellow_taxi_jan_25_2018"
        / "part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet",
        data_dir / "numbers.csv",
        data_dir / "sampleTextFile.txt",
        data_dir / "cars_dates.json",
        data_dir / "taxi_zones.csv",
        (data_dir / "stocks.csv"),
    )
    return expected_files


def load_files() -> tuple[dict[str, BytesIO], dict[str, LoadErrorType]]:
    """
    Loads files from the expected files list.
    Returns a tuple with successfully loaded files and files with errors.
    """
    expected_files = list_expected_resource_files()
    loaded_files: dict[str, BytesIO] = {}
    file_w_errors: dict[str, LoadErrorType] = {}

    for fp in expected_files:
        if not fp.is_file():
            file_w_errors[str(fp)] = "missing_file"
        else:
            try:
                data = BytesIO(fp.read_bytes())
                loaded_files[str(fp)] = data
            except Exception:
                file_w_errors[str(fp)] = "error_loading"

    if any(file_w_errors):
        raise ResourceLoadingError(file_w_errors)

    return loaded_files, file_w_errors
