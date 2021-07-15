"""Common variables and functions that may be imported by any module"""
from typing import Union
from pathlib import Path

STORAGE = Path(__file__).parent / "storage"
AIS_DB = STORAGE / "ais.db"
ONC_DB = STORAGE / "onc.db"
ONC_DIR = STORAGE / "onc"
AIS_TEMP_DIR = STORAGE / "onc"
LOCAL_TOKEN_PATH = STORAGE / "token"


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


def save_user_token(token: Union[Path, str], force: bool = False) -> None:
    """Save the provided ONC token so future functions that require an
    ONC token will use this value by default.

    Arguments:
        token: the token string or location of the file.  If a file, the
            first line must contain the token and nothing else.
        force: whether to overwrite saved token if it exists
    """
    if LOCAL_TOKEN_PATH.exists() and not force:
        raise OSError(
            "A saved token already exists.  If you want to"
            + "overwrite, pass `force=True`."
        )
    if Path(token).exists():
        with open(Path(token), "r") as fh:
            token = fh.readline()
    with open(LOCAL_TOKEN_PATH, "w") as fh:
        fh.write(token)


def load_user_token() -> str:
    """Load saved token"""
    with open(LOCAL_TOKEN_PATH, "r") as fh:
        return fh.readline()
