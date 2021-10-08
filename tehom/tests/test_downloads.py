from tehom import downloads, _persistence


def test_download_ais_to_temp(declare_stateful):
    year = 2014
    month = 1
    zone = 1
    downloads._download_ais_to_temp(year, month, zone)
    path = _persistence.AIS_TEMP_DIR / f"{year}_{month}_{zone}.zip"
    assert path.exists()
