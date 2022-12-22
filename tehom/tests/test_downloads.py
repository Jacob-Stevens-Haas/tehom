import pandas as pd
import pytest

from sqlalchemy import text

from tehom import downloads, _persistence


@pytest.mark.slow
def test_download_ais_to_temp(declare_stateful):
    year = 2014
    month = 1
    zone = 7
    path = downloads._download_ais_to_temp(year, month, zone)
    assert path.exists()


@pytest.fixture
def ais2016_01_07(declare_stateful):
    """Download a month of AIS data as a zipfile and return its path"""
    yield downloads._download_ais_to_temp(2016, 1, 7)


@pytest.mark.slow
def test_unzip_ais(declare_stateful, ais2016_01_07):
    zip_tree, zip_file = downloads._unzip_ais(ais2016_01_07)
    assert zip_file.exists()
    assert zip_tree in zip_file.parents


@pytest.fixture
def complete_ship_download(declare_stateful):
    downloads.download_ships(2016, 1, 7)  # Zone 7 generates smallest files.


@pytest.mark.slow
def test_integrated_metadata_updated(complete_ship_download):
    assert (2016, 1, 7) in _persistence.get_ais_downloads(_persistence.AIS_DB)


@pytest.mark.slow
def test_integrated_ships_updated(complete_ship_download):
    eng = _persistence._get_engine(_persistence.AIS_DB)
    result = eng.execute(text("SELECT COUNT(*) FROM ships;")).fetchall()
    assert result[0][0] == 1779


@pytest.mark.slow
def test_integration_temps_removed(complete_ship_download):
    files = _persistence.AIS_TEMP_DIR.iterdir()
    newitem = next(files, None)
    assert newitem is None


@pytest.mark.slow
def test_acoustic_files_downloaded(declare_stateful):
    onc_folder = _persistence.ONC_DIR
    downloads.download_acoustics(
        ["ICLISTENHF1251"],
        "2016-06-20T12:00:00.000Z",
        "2016-06-20T12:01:00.000Z",
        "wav",
    )
    files = (file.name for file in onc_folder.iterdir())
    assert "ICLISTENHF1251_20160620T115632.000Z.wav" in files


@pytest.mark.slow
def test_acoustic_files_downloaded_no_diversion_mode(declare_stateful):
    # See https://github.com/OceanNetworksCanada/api-python-client/issues/4
    onc_folder = _persistence.ONC_DIR
    downloads.download_acoustics(
        ["ICLISTENHF1252"],
        "2016-01-01T12:00:00.000Z",
        "2016-01-01T12:01:00.000Z",
        "wav",
    )
    files = (file.name for file in onc_folder.iterdir())
    assert "ICLISTENHF1252_20160101T115623.000Z.wav" in files


def test_show_bar(declare_stateful):
    downloads.show_available_data(begin="2016-06", end="2016-08", style="bar")


def test_show_map(declare_stateful):
    downloads.show_available_data(begin="2016-06", end="2016-08", style="map")


def test_show_bar_avail(declare_stateful):
    downloads.show_available_data(
        begin="2016-06", end="2016-08", style="bar", certified=True
    )


def test_onc_iso_fmt_tz_handling():
    expected = "2016-06-20T12:00:00.000Z"
    dt_utc = "2016-06-20T12:00:00.000Z"
    result = downloads._onc_iso_fmt(dt_utc)
    assert result == expected
    dt = "2016-06-20T12:00:00.000"
    result = downloads._onc_iso_fmt(dt)
    assert result == expected


@pytest.fixture
def mock_load_datetime():
    return pd.Timestamp("20160401T000001Z")


@pytest.fixture
def mock_deployments(mock_load_datetime):
    temp_loaded_datetime = downloads.MODULE_LOADED_DATETIME
    downloads.MODULE_LOADED_DATETIME = mock_load_datetime
    hphones = pd.DataFrame(
        {
            "deviceCode": ["a", "a", "b"],
            "begin": [
                pd.Timestamp("2016"),
                pd.Timestamp("20160201"),
                pd.Timestamp("20160301"),
            ],
            "end": [pd.Timestamp("20160115T000001Z"), None, None],
        }
    )
    yield hphones
    downloads.MODULE_LOADED_DATETIME = temp_loaded_datetime


def test_what_to_certify_nothing_yet(mock_deployments, mock_load_datetime):
    result = downloads._what_to_certify(mock_deployments, pd.DataFrame())
    expected = pd.DataFrame(
        {
            "deviceCode": ["a", "a", "b"],
            "begin": [
                pd.Timestamp("2016"),
                pd.Timestamp("20160201"),
                pd.Timestamp("20160301"),
            ],
            "end": [
                pd.Timestamp("20160115T000001Z"),
                mock_load_datetime,
                mock_load_datetime,
            ],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_what_to_certify_overlap(mock_deployments, mock_load_datetime):
    mock_processed_df = pd.DataFrame(
        {
            "deviceCode": ["a", "a"],
            "begin": [pd.Timestamp("2016"), pd.Timestamp("20160201")],
            "end": [
                pd.Timestamp("20160115T000001Z"),
                pd.Timestamp("20160215T000001Z"),
            ],
        }
    )

    result = downloads._what_to_certify(mock_deployments, mock_processed_df)
    expected = pd.DataFrame(
        {
            "deviceCode": ["a", "b"],
            "begin": [
                pd.Timestamp("20160215T000001Z"),
                pd.Timestamp("20160301"),
            ],
            "end": [mock_load_datetime, mock_load_datetime],
        }
    )
    pd.testing.assert_frame_equal(result, expected)
