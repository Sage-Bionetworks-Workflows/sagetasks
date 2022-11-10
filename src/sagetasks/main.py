from typer import Typer

import sagetasks.nextflowtower.typer

main_app = Typer(rich_markup_mode="markdown")
main_app.add_typer(sagetasks.nextflowtower.typer.app, name="nextflowtower")
