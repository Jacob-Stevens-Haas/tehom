import pytest

from tehom._persistence import test_storage


@pytest.fixture
def declare_stateful():
    with test_storage():
        yield None
