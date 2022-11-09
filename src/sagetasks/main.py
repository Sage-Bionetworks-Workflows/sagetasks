from typer import Typer

import sagetasks.nextflowtower.typer

app = Typer()
app.add_typer(sagetasks.nextflowtower.typer.app, name="nextflowtower")
