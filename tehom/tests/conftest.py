import shutil

import pytest

from tehom import downloads, _persistence


@pytest.fixture
def declare_stateful():
    with _persistence.test_storage():
        if _persistence.STORAGE.exists():
            shutil.rmtree(_persistence.STORAGE)
        _persistence.init_data_folder()
        yield None
        shutil.rmtree(_persistence.STORAGE)


@pytest.fixture
def complete_acoustic_download(declare_stateful):
    downloads.download_acoustics(
        ["ICLISTENHF1252"], "20160101T12:00:00", "20160101T12:01:00", "wav"
    )
