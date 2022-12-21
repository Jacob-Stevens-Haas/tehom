import pandas as pd
import pytest

from spans import datetimerange
from sqlalchemy import Table, MetaData, select, and_

from tehom import _persistence


def test_stateful_switch(declare_stateful):
    temp_path = _persistence.STORAGE

    result = temp_path.stem
    expected = "test_storage"

    assert result == expected


def test_query_ais_no_table(declare_stateful):
    result = _persistence.get_ais_downloads()
    assert result == []


def test_query_onc_no_table(declare_stateful):
    result = _persistence.get_onc_downloads()
    assert result.empty


def test_save_token(declare_stateful):
    expected = "hahaha"
    _persistence.save_user_token(expected)
    result = _persistence.load_user_token()

    assert result == expected


def test_ais_init(declare_stateful):
    md = _persistence.init_ais_db(_persistence.AIS_DB)
    tb = md.tables["meta"]
    stmt = tb.insert([1, 1, 1])
    stmt.execute()
    stmt = tb.select()
    result = stmt.execute().fetchall()
    assert len(result) == 1


def test_onc_init(declare_stateful):
    md = _persistence.init_onc_db(_persistence.ONC_DB)
    tb = md.tables["spans"]
    stmt = tb.insert(["a", "a", "a", "a"])
    stmt.execute()
    stmt = tb.select()
    result = stmt.execute().fetchall()
    assert len(result) == 1


@pytest.fixture
def default_engine():
    onc_db = _persistence.ONC_DB
    yield _persistence._get_engine(onc_db)


@pytest.fixture
def onc_files_table(default_engine):
    md = MetaData(default_engine)
    files_table = Table("files", md, *_persistence._onc_files_columns())  # noqa: F841
    yield files_table


@pytest.fixture
def onc_spans_table(default_engine):
    md = MetaData(default_engine)
    spans_table = Table("spans", md, *_persistence._onc_spans_columns())  # noqa: F841
    yield spans_table


@pytest.mark.slow
def test_files_table_updated(
    complete_acoustic_download, default_engine, onc_files_table
):
    stmt = select(onc_files_table).where(
        and_(
            onc_files_table.c.hydrophone == "ICLISTENHF1252",
            onc_files_table.c.format == "wav",
            onc_files_table.c.start == "20160101T115623.000Z",
            onc_files_table.c.filename == "ICLISTENHF1252_20160101T115623.000Z.wav",
        )
    )
    results = default_engine.execute(stmt).fetchone()
    assert results


@pytest.mark.slow
def test_spans_table_updated(
    complete_acoustic_download, default_engine, onc_spans_table
):
    stmt = select(onc_spans_table.c.start, onc_spans_table.c.finish).where(
        and_(
            onc_spans_table.c.hydrophone == "ICLISTENHF1252",
            onc_spans_table.c.format == "wav",
        )
    )
    spans_df = pd.read_sql(stmt, default_engine)
    ranges = _persistence.datetimerangeset_from_df(spans_df)
    expected = datetimerange(
        pd.to_datetime("2016-01-01 11:56:23.000000"),
        pd.to_datetime("2016-01-01 12:01:23.000000"),
    )
    assert expected in ranges
