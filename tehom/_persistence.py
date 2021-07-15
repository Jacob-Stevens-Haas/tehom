"""Common variables and functions that may be imported by any module"""
from typing import Union
from pathlib import Path

STORAGE = Path(__file__) / "storage"
AIS_DB = STORAGE / "ais.db"
ONC_DB = STORAGE / "onc.db"
ONC_DIR = STORAGE / "onc"
AIS_TEMP_DIR = STORAGE / "ais"


def _init_data_folder():
    """Initializes the data folder if it doesn't exist."""
    if not STORAGE.exists() and STORAGE.is_dir():
        STORAGE.mkdir()
    elif STORAGE.exists() and not STORAGE.is_dir():
        raise OSError("{} exists but is not a directory.".format(str(STORAGE)))
    if not ONC_DIR.exists() and ONC_DIR.is_dir():
        ONC_DIR.mkdir()
    elif ONC_DIR.exists() and not ONC_DIR.is_dir():
        raise OSError("{} exists but is not a directory.".format(str(ONC_DIR)))
    if not AIS_TEMP_DIR.exists() and AIS_TEMP_DIR.is_dir():
        AIS_TEMP_DIR.mkdir()
    elif AIS_TEMP_DIR.exists() and not AIS_TEMP_DIR.is_dir():
        raise OSError(f"{AIS_TEMP_DIR} exists but is not a directory.")
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
