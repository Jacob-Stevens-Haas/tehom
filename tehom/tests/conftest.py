import pytest

from tehom import _persistence


@pytest.fixture
def declare_stateful():
    with _persistence.test_storage():
        _persistence._init_data_folder()
        yield None
