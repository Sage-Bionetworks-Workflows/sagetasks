from typer import Typer

import sagetasks.nextflowtower.general as general
from sagetasks.utils import to_typer_commands

# Auto-generate Typer commands from general functions
app = Typer()
to_typer_commands(general, app)
