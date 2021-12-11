"""Common variables and functions that may be imported by any module"""
from contextlib import contextmanager
from typing import Union, List, Tuple
from pathlib import Path

import sqlalchemy

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    Float,
    String,
    MetaData,
    Index,
    select,
)

STORAGE = Path(__file__).parent / "storage"
AIS_DB = STORAGE / "ais.db"
ONC_DB = STORAGE / "onc.db"
ONC_DIR = STORAGE / "onc"
AIS_TEMP_DIR = STORAGE / "ais"
LOCAL_TOKEN_PATH = STORAGE / "token"


@contextmanager
def test_storage():
    global STORAGE
    global AIS_DB
    global ONC_DB
    global ONC_DIR
    global AIS_TEMP_DIR
    global LOCAL_TOKEN_PATH

    temp_storage = STORAGE
    temp_ais_db = AIS_DB
    temp_onc_db = ONC_DB
    temp_onc_dir = ONC_DIR
    temp_ais_temp_dir = AIS_TEMP_DIR
    temp_token_path = LOCAL_TOKEN_PATH

    STORAGE = Path(__file__).parent / "test_storage"
    AIS_DB = STORAGE / "ais.db"
    ONC_DB = STORAGE / "onc.db"
    ONC_DIR = STORAGE / "onc"
    AIS_TEMP_DIR = STORAGE / "ais"
    LOCAL_TOKEN_PATH = STORAGE / "token"

    yield None

    STORAGE = temp_storage
    AIS_DB = temp_ais_db
    ONC_DB = temp_onc_db
    ONC_DIR = temp_onc_dir
    AIS_TEMP_DIR = temp_ais_temp_dir
    LOCAL_TOKEN_PATH = temp_token_path


def init_data_folder():
    """Initializes the data folder if it doesn't exist."""
    if not STORAGE.exists():
        STORAGE.mkdir()
    elif not STORAGE.is_dir():
        raise OSError("{} exists but is not a directory.".format(str(STORAGE)))
    if not ONC_DIR.exists():
        ONC_DIR.mkdir()
    elif not ONC_DIR.is_dir():
        raise OSError("{} exists but is not a directory.".format(str(ONC_DIR)))
    if not AIS_TEMP_DIR.exists():
        AIS_TEMP_DIR.mkdir()
    elif not AIS_TEMP_DIR.is_dir():
        raise OSError(f"{AIS_TEMP_DIR} exists but is not a directory.")
    pass


def _get_engine(db: Path) -> sqlalchemy.engine.base.Engine:
    return create_engine("sqlite:///" + str(db))


def init_ais_db(ais_db: Union[Path, str]) -> MetaData:
    """Initializes the local AIS record database, if it does not exist"""
    eng = _get_engine(ais_db)
    md = MetaData(eng)
    meta_table = Table("meta", md, *_ais_meta_columns())  # noqa: F841
    ships_table = Table("ships", md, *_ais_ships_columns())  # noqa: F841
    md.create_all()
    return md


def _ais_meta_columns():
    return [
        Column("year", Integer, primary_key=True),
        Column("month", Integer, primary_key=True),
        Column("zone", Integer, primary_key=True),
    ]


def _ais_ships_columns():
    return [
        Column("mmsi", Integer, primary_key=True),
        Column("BaseDateTime", String, primary_key=True),
        Column("lat", Float),
        Column("lon", Float),
        Column("sog", Float),
        Column("cog", Float),
        Column("heading", Float),
        Column("vesselname", String),
        Column("callsign", String),
        Column("vesseltype", Integer),
        Column("status", String),
        Column("length", Float),
        Column("width", Float),
        Column("draft", Float),
        Column("cargo", Integer),
        Index("idx_time_lat_lon", "BaseDateTime", "lat", "lon"),
    ]


def init_onc_db(onc_db: Union[Path, str]) -> None:
    """Initializes the local ONC record database, if it does not exist"""
    eng = _get_engine(onc_db)
    md = MetaData(eng)
    spans_table = Table("spans", md, *_onc_spans_columns())  # noqa: F841
    files_table = Table("files", md, *_onc_files_columns())  # noqa: F841
    md.create_all()
    return md


def _onc_spans_columns():
    return [
        Column("hydrophone", String, primary_key=True),
        Column("start", String, primary_key=True),
        Column("finish", String, primary_key=True),
        Column("format", String, primary_key=True),
    ]


def _onc_files_columns():
    return [
        Column("hydrophone", String, primary_key=True),
        Column("start", String, primary_key=True),
        Column("duration", Integer),
        Column("format", String),
        Column("filename", String),
    ]


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
            token = fh.readline().strip()
    with open(LOCAL_TOKEN_PATH, "w") as fh:
        fh.write(token)


def load_user_token() -> str:
    """Load saved token"""
    with open(LOCAL_TOKEN_PATH, "r") as fh:
        return fh.readline()


def _get_ais_downloads(ais_db: Union[Path, str]) -> List[Tuple]:
    """Identify which AIS year-month-zone combinations have already been
    added to the AIS database

    Arguments:
        ais_db: path to the database of AIS records

    Returns:
        set of records, each arragned as a tuple comprising (year,
        month, zone)
    """
    eng = _get_engine(ais_db)
    md = MetaData(eng)
    meta_table = Table("meta", md, *_ais_meta_columns())  # noqa: F841
    stmt = select(meta_table)
    return eng.execute(stmt).fetchall()
