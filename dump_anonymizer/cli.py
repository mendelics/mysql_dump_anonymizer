import json
import logging
import sys
from typing import Annotated

import typer

from main import read_dump_table_structure, read_dump_inserts, anonymize, write_in_file
from models import TableChangeSettings

app = typer.Typer()


@app.command()
def default(
    input_file: Annotated[
        str,
        typer.Option(
            "--input",
            "-i",
            help="The source dump filepath to read from. Use `-` for stdin.",
        ),
    ] = None,
    config_file: Annotated[
        str,
        typer.Option(
            "--configuration",
            "-c",
            help="The configuration file to use during anonymization."
        ),
    ] = None,
    output_file: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="The output file to save anonymized dump."
        ),
    ] = "output.sql",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            help="Increases the verbosity of the logging feature, to help when troubleshooting issues.",
        ),
    ] = False,
):
    root_logger = logging.getLogger()

    loglevel = logging.INFO
    if verbose:
        loglevel = logging.DEBUG

    root_logger.handlers.clear()
    console_handler = logging.StreamHandler(sys.stderr)

    root_logger.setLevel(loglevel)
    root_logger.addHandler(console_handler)

    with open(input_file, "r"):
        print(f"File {input_file} successfully opened")

    if config_file:
        with open(config_file) as settings_file:
            settings = json.load(settings_file)
        print(f"Config file '{config_file}' successfully loaded")
    else:
        raise Exception("No config file provided")

    parsed_settings = [TableChangeSettings.model_validate(table) for table in settings["tables"]]

    tables_structure = read_dump_table_structure(input_file)
    inserts = read_dump_inserts(input_file, tables_structure)
    inserts = anonymize(inserts, tables_structure, parsed_settings)

    print(f"Writing changes in file {output_file}")
    write_in_file(input_file, output_file, inserts)
    print(f"Anonymized dump successfully written to {output_file}")


def cli():
    app()


if __name__ == "__main__":
    cli()
