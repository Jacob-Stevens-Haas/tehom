from tehom import _persistence


def test_stateful_switch(declare_stateful):
    temp_path = _persistence.STORAGE

    result = temp_path.stem
    expected = "test_storage"

    assert result == expected


def test_save_token(declare_stateful):
    expected = "hahaha"
    _persistence.save_user_token(expected)
    result = _persistence.load_user_token()

    assert result == expected
