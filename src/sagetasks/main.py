from typer import Typer

import sagetasks.nextflowtower.typer as nextflowtower

app = Typer()
app.add_typer(nextflowtower.app, name="nextflowtower")
