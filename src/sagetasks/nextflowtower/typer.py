import sagetasks.nextflowtower.general
from sagetasks.utils import to_typer_commands

# Auto-generate Typer commands from general functions
app = to_typer_commands(sagetasks.nextflowtower.general)
