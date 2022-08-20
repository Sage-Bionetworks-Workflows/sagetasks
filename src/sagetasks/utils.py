import inspect
import sys

from prefect import task


def to_prefect_tasks(module_name, general_module):
    this_module = sys.modules[module_name]
    general_funcs = inspect.getmembers(general_module, inspect.isfunction)
    for name, func in general_funcs:
        docstring = getattr(func, "__doc__", "")
        first_line = docstring.splitlines()[0]
        task_func = task(func, name=first_line)
        setattr(this_module, name, task_func)
