import inspect
import sys
from collections.abc import Mapping, Sequence
from copy import copy

from prefect import task


def to_prefect_tasks(module_name, general_module):
    this_module = sys.modules[module_name]
    general_funcs = inspect.getmembers(general_module, inspect.isfunction)
    for name, func in general_funcs:
        docstring = getattr(func, "__doc__", "")
        first_line = docstring.splitlines()[0]
        task_func = task(func, name=first_line)
        setattr(this_module, name, task_func)


def update_dict(base_dict, overrides):
    dict_copy = copy(base_dict)
    for k, v in overrides.items():
        oldv = dict_copy.get(k, {})
        if k not in dict_copy:
            valid = set(dict_copy)
            raise ValueError(f"Cannot update {k}. Not among {valid}.")
        elif isinstance(oldv, Mapping) and isinstance(v, Mapping):
            dict_copy[k] = update_dict(oldv, v)
        elif v is not None:
            dict_copy[k] = v
    return dict_copy


def dedup(x):
    if isinstance(x, Sequence):
        x = list(set(x))
    return x
