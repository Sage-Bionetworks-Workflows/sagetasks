from sagetasks.utils import to_prefect_tasks
import sagetasks.sevenbridges.general as general

# Auto-generate Prefect tasks from general functions
to_prefect_tasks(__name__, general)
