import importlib
import importlib.resources
import logging
import os
from importlib.abc import Traversable
from io import BytesIO
from pathlib import Path
from typing import Literal, get_args

import appdirs

import spark_rock_jvm_python.resources.data as _data_resources
from spark_rock_jvm_python.config import APP_NAME

logger = logging.getLogger(__name__)


class ResourceLoadingError(Exception):
    pass


class ResourceDuplicateError(Exception):
    def __init__(self, resource_name: "ResourceFile | str"):
        message = f"{resource_name} name already exists in {__package__} pre-loaded data files. Reserved names: {get_args(ResourceFile)}"
        super().__init__(message)


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
    """Get directory to package resources with pre-included data files"""
    data_dir = importlib.resources.files(_data_resources)
    return data_dir


def make_data_dir_read_only() -> None:
    data_dir = Path(str(get_data_dir()))
    for root, dirs, files in os.walk(data_dir):
        for d in dirs:
            os.chmod(os.path.join(root, d), 0o500)
        for f in files:
            os.chmod(os.path.join(root, f), 0o444)


def get_writes_dir() -> Path:
    """Use app dirs to find appropriate user data writes directory"""
    writes_dir = Path(appdirs.user_data_dir(APP_NAME))
    return writes_dir


def resource_path(resource: ResourceFile) -> Path:
    _dir = get_data_dir()
    if (traversable := _dir / resource).is_file():
        return Path(str(traversable))
    elif resource == "yellow_taxi_jan_25_2018":
        return Path(str(_dir / resource))
    else:
        raise FileNotFoundError(traversable)


def write_path(file_name: str, /) -> Path:
    write_dir = get_writes_dir()
    if file_name in set(get_args(ResourceFile)):
        raise ResourceDuplicateError(file_name)
    return write_dir / file_name


def print_write_path_contents(file: str | Path, /, indent=0) -> None:
    """Note that when writing dataframes with spark, a folder is created with multiple files"""
    written_path = write_path(file) if isinstance(file, str) else file
    if not (written_path).exists():
        raise NotADirectoryError(write_path)
    for f in os.listdir(written_path):
        print(" " * indent + f)


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
