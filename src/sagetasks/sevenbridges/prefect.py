import sagetasks.sevenbridges.general
from sagetasks.utils import to_prefect_tasks

# Auto-generate Prefect tasks from general functions
to_prefect_tasks(__name__, sagetasks.sevenbridges.general)
