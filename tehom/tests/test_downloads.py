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
