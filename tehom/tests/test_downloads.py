import pytest

from tehom import downloads


@pytest.mark.slow
def test_download_ais_to_temp(declare_stateful):
    year = 2014
    month = 1
    zone = 1
    path = downloads._download_ais_to_temp(year, month, zone)
    assert path.exists()


@pytest.fixture
def ais2016_01_01(declare_stateful):
    """Download a month of AIS data as a zipfile and return its path"""
    yield downloads._download_ais_to_temp(2016, 1, 1)


@pytest.mark.slow
def test_unzip_ais(declare_stateful, ais2016_01_01):
    zip_tree, zip_file = downloads._unzip_ais(ais2016_01_01)
    assert zip_file.exists()
    assert zip_tree in zip_file.parents
