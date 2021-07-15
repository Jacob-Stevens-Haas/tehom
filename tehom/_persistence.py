"""Common variables and functions that may be imported by any module"""
from typing import Union
from pathlib import Path

STORAGE = Path(__file__) / "storage"
AIS_DB = STORAGE / "ais.db"
ONC_DB = STORAGE / "onc.db"
ONC_DIR = STORAGE / "onc"
AIS_TEMP_DIR = STORAGE / "onc"


def _init_data_folder():
    """Initializes the data folder if it doesn't exist."""
    # see `STORAGE`, `AIS_TEMP_DIR`, and `ONC_DIR` globals.
    pass


def _init_ais_db(ais_db: Union[Path, str]) -> None:
    """Initializes the local AIS record database, if it does not exist"""
    # see `AIS_DB` global
    # Should have two tables:
    #    meta: year, month, zone
    #    data: mimic columns in downloads from https://marinecadastre.gov/ais/
    #      date-time column needs an index
    pass


def _init_onc_db(onc_db: Union[Path, str]) -> None:
    """Initializes the local AIS record database, if it does not exist"""
    # see `ONC_DB` global
    # Should have three tables:
    #    meta: hydrophone, start, finish, format
    #    files: hydrophone, start, duration, format, filename
    #    deployments: mimics return columns of onc.getDeployments()
    pass
