import pytest

from tehom import downloads


@pytest.mark("slow")
def test_download_ais_to_temp(declare_stateful):
    year = 2014
    month = 1
    zone = 1
    path = downloads._download_ais_to_temp(year, month, zone)
    assert path.exists()
