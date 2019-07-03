"""
SimDash's command line interface
"""
#pylint: disable=unused-import
#pylint: disable=unexpected-keyword-arg

import logbook
import click_completion

from . import cli_main

import simdash.serve

if __name__ == "__main__":
    click_completion.init()
    if __debug__:
        logbook.StderrHandler(logbook.DEBUG).push_application()
    else:
        logbook.StderrHandler(logbook.INFO).push_application()
    logbook.compat.redirect_logging()
    cli_main(prog_name="simdash")
