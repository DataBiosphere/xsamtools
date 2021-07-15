"""This is the main CLI entry point."""
import sys

# Only the commands modules imported here are available to the CLI
# These must be imported before dispatch
import xsamtools.cli.cram
import xsamtools.cli.vcf
from xsamtools.cli import dispatch


def main():
    if 2 == len(sys.argv) and "--version" == sys.argv[1].strip():
        from xsamtools import version
        print(version.__version__)
    else:
        dispatch(sys.argv[1:])
