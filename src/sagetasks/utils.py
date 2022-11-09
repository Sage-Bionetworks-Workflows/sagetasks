import inspect
import sys
from collections.abc import Mapping, Sequence
from copy import copy
from functools import wraps

from prefect import task
from rich import print as rich_print
from typer import Typer


def to_prefect_tasks(module_name: str, general_module: str) -> None:
    """Wrap functions inside a general module as Prefect tasks.

    Args:
        module_name (str): Module name.
        general_module (str): General submodule name.
    """
    this_module = sys.modules[module_name]
    general_funcs = inspect.getmembers(general_module, inspect.isfunction)
    for name, func in general_funcs:
        docstring = getattr(func, "__doc__", "")
        first_line = docstring.splitlines()[0]
        task_func = task(func, name=first_line)
        setattr(this_module, name, task_func)


def to_typer_commands(general_module: str) -> None:
    """Wrap functions inside a general module as Typer commands.

    Most functions being converted into Typer commands have
    a return value. In Python, that return value can be used
    for other purposes. At the CLI, this return value isn't
    visible by default. Hence, before being passed to Typer,
    `to_typer_commands()` wraps each function such that the
    return value is printed on standard output. For the time
    being, the `print()` function from the `rich` package is
    being used for colored and formatted output.

    Args:
        general_module (str): General submodule name.
    """

    # This weird setup is to avoid a flake8 B023 linting
    # error, which is associated with the following gotcha:
    # https://docs.python-guide.org/writing/gotchas/#late-binding-closures
    def add_print(func):
        @wraps(func)
        def printing_func(*args, **kwargs):
            result = func(*args, **kwargs)
            rich_print(result)
            # TODO: Use this after we add a global JSON output CLI option
            # try:
            #     output = json.dumps(result, indent=2)
            # except TypeError:
            #     output = repr(result)
            # print(output)

        return printing_func

    typer_app = Typer(rich_markup_mode="markdown")
    general_funcs = inspect.getmembers(general_module, inspect.isfunction)

    for _, func in general_funcs:
        printing_func = add_print(func)
        typer_app.command()(printing_func)

    return typer_app


def update_dict(base_dict: Mapping, overrides: Mapping) -> Mapping:
    """Update a dictionary recursively with a set of overrides.

    Args:
        base_dict (Mapping): Base dictionary.
        overrides (Mapping): Dictionary with overrides.

    Raises:
        ValueError: If there is an attempt to create a new key.

    Returns:
        Mapping: Updated dictionary.
    """
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


def dedup(x: Sequence) -> list:
    """Deduplicate elements in a sequence (such as a list).

    Args:
        x (Sequence): List of elements.

    Returns:
        list: Deduplicated list of elements.
    """
    if isinstance(x, Sequence):
        x = list(set(x))
    return x
