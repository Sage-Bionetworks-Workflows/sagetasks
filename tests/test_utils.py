from copy import deepcopy

import pytest

from sagetasks.utils import update_dict

EG_DICT = {
    "foo": [1, 2, 3],
    "bar": None,
    "baz": {"tic": 1.1, "tac": 2.2, "toe": 3.3},
}


def test_update_dict_none():
    # Expecting no change due to None value
    overrides = {"foo": None}
    result = update_dict(EG_DICT, overrides)
    assert result == EG_DICT


def test_update_dict_invalid():
    # Expecting exception from updating missing key
    overrides = {"oof": "gah"}
    with pytest.raises(ValueError):
        update_dict(EG_DICT, overrides)


def test_update_dict_toplevel():
    # Expecting top-level changes
    overrides = {"foo": [4, 5, 6], "bar": False}
    result = update_dict(EG_DICT, overrides)
    assert result["foo"] == overrides["foo"]
    assert result["bar"] == overrides["bar"]


def test_update_dict_nested():
    # Expecting nested changes
    overrides = {"baz": {"tic": 7.7, "tac": {"some": "thing"}}}
    result = update_dict(EG_DICT, overrides)
    assert result["baz"]["tic"] == overrides["baz"]["tic"]
    assert result["baz"]["tac"] == overrides["baz"]["tac"]
    assert result["baz"]["toe"] == 3.3


def test_update_dict_copy():
    # Expecting input dictionary to not change
    eg_dict_copy = deepcopy(EG_DICT)
    overrides = {"bar": "random"}
    result = update_dict(EG_DICT, overrides)
    assert result["bar"] == overrides["bar"]
    assert EG_DICT == eg_dict_copy
