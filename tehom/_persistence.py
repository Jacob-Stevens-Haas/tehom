"""Common variables and functions that may be imported by any module"""
import warnings

from contextlib import contextmanager
from typing import Union, List, Tuple, Set
from pathlib import Path

import pandas as pd
import sqlalchemy

from onc.onc import ONC
from spans import datetimerange, datetimerangeset
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
    insert,
    delete,
    and_,
)

STORAGE = Path(__file__).parent / "storage"
AIS_DB = STORAGE / "ais.db"
ONC_DB = STORAGE / "onc.db"
ONC_DIR = STORAGE / "onc"
AIS_TEMP_DIR = STORAGE / "ais"
LOCAL_TOKEN_PATH = STORAGE / "token"


def load_user_token() -> str:
    """Load saved token"""
    with open(LOCAL_TOKEN_PATH, "r") as fh:
        return fh.readline()


try:
    onc_session = ONC(
        load_user_token(),
        showInfo=True,
        outPath=str(ONC_DIR),
    )
except FileNotFoundError:
    warnings.warn(
        "Module loaded with no ONC token; unable to query ONC server data."
    )


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
    try:
        onc_session.outPath = str(ONC_DIR)
    except NameError:
        pass

    yield None

    STORAGE = temp_storage
    AIS_DB = temp_ais_db
    ONC_DB = temp_onc_db
    ONC_DIR = temp_onc_dir
    AIS_TEMP_DIR = temp_ais_temp_dir
    LOCAL_TOKEN_PATH = temp_token_path
    try:
        onc_session.outPath = str(ONC_DIR)
    except NameError:
        pass


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
        Column("basedatetime", String, primary_key=True),
        Column("lat", Float),
        Column("lon", Float),
        Column("sog", Float),
        Column("cog", Float),
        Column("heading", Float),
        Column("vesselname", String),
        Column("imo", String),
        Column("callsign", String),
        Column("vesseltype", Integer),
        Column("status", String),
        Column("length", Float),
        Column("width", Float),
        Column("draft", Float),
        Column("cargo", Integer),
        Index("idx_time_lat_lon", "basedatetime", "lat", "lon"),
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


def get_ais_downloads(ais_db: Union[Path, str]) -> List[Tuple]:
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


def update_ais_downloads(year, month, zone, ais_db):
    """Updates the AIS database to track downloads

    Arguments:
        year (int): year to download
        month (int): month to download
        zone (int): UTM zone to download
        ais_db: path to the database of AIS records
    """
    eng = _get_engine(ais_db)
    md = MetaData(eng)
    meta_table = Table("meta", md, *_ais_meta_columns())  # noqa: F841
    stmt = insert(meta_table).values(year=year, month=month, zone=zone)
    return eng.execute(stmt)


def update_onc_tracker(onc_db: Path, files: List[str], format) -> None:
    """Updates the ONC database to track downloads

    Arguments:
        onc_db: database to track ONC downloads
        files: list of files downloaded to add to the tracker
    """

    def _extract_info_from_filename(file: str) -> Tuple:
        pieces = file.split("_", 1)
        hydrophone = pieces[0]
        start = pieces[1].rsplit(".", 1)[0]
        return (hydrophone, start, file)

    file_tuples = [
        _extract_info_from_filename(file)
        for file in files
        if file[-3:] == format
    ]
    file_df = pd.DataFrame(
        data=file_tuples, columns=["hydrophone", "start", "filename"]
    )
    file_df["format"] = format
    file_df["duration"] = 300000

    eng = _get_engine(onc_db)
    file_df.to_sql("files", eng, if_exists="append", index=False)

    file_df["start"] = pd.to_datetime(file_df["start"])
    duration = file_df["duration"].apply(lambda d: pd.Timedelta(d, "ms"))
    file_df["finish"] = file_df["start"] + duration

    md = MetaData(eng)
    spans_table = Table("spans", md, *_onc_spans_columns())  # noqa: F841
    hphone_gb = file_df.groupby("hydrophone")
    for hphone, h_df in hphone_gb:
        _record_downloaded_intervals_onc(
            hphone, h_df, format, spans_table, eng
        )


def _record_downloaded_intervals_onc(
    hphone: str,
    h_df: pd.DataFrame,
    format: str,
    spans_table: Table,
    eng: sqlalchemy.engine.base.Engine,
) -> None:
    """Calculate and update intervals where acoustic data is available.

    Arguments:
        hphone: the hydrophone concerned
        h_df: DataFrame of individual files added
        format: file format concerned
        spans_table: Table object for existing calculated intervals
        eng: capable of generating connections for CRUD
    """
    and_clause = and_(
        spans_table.c.hydrophone == hphone,
        spans_table.c.format == format,
    )
    stmt = select(spans_table.c.start, spans_table.c.finish).where(and_clause)
    spans_df = pd.read_sql(stmt, eng)

    old_ranges = datetimerangeset_from_df(spans_df)
    new_ranges = datetimerangeset_from_df(h_df).union(old_ranges)
    del_old_stmt = delete(spans_table).where(and_clause)
    new_df = pd.DataFrame(
        [(r.lower, r.upper) for r in new_ranges], columns=["start", "finish"]
    )
    new_df["hydrophone"] = hphone
    new_df["format"] = format

    with eng.begin() as conn:
        conn.execute(del_old_stmt)
        new_df.to_sql("spans", conn, if_exists="append", index=False)


def datetimerangeset_from_df(df):
    if df.empty:
        return datetimerangeset([])
    df["start"] = pd.to_datetime(df["start"])
    df["finish"] = pd.to_datetime(df["finish"])
    ranges = df.loc[:, ["start", "finish"]].apply(
        lambda row: datetimerange(row["start"], row["finish"]), axis=1
    )
    return datetimerangeset(ranges)


def _get_onc_downloads(onc_db: Path) -> Set:
    """Identify which ONC hydrophone data ranges have been downloaded
    and tracked in the ONC database.

    Arguments:
        onc_db: path to the database of ONC records

    Returns:
        DataFrame of records for each hydrophone's downloaded data range
        for each format of download.
    """
    eng = _get_engine(onc_db)
    md = MetaData(eng)
    spans_table = Table("spans", md, *_onc_spans_columns())  # noqa: F841
    return pd.read_sql(select(spans_table), eng)
