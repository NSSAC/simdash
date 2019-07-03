"""
Hello world command.
"""

import click

from . import cli_main

@cli_main.command()
def hello():
    """
    Prints "Hello, world!"
    """

    click.echo("Hello, world!")
