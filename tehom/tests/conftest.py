import shutil

import pytest

from tehom import _persistence


@pytest.fixture
def declare_stateful():
    with _persistence.test_storage():
        if _persistence.STORAGE.exists():
            shutil.rmtree(_persistence.STORAGE)
        _persistence._init_data_folder()
        yield None
        shutil.rmtree(_persistence.STORAGE)
