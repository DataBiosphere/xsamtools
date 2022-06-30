"""
CRAM file utilities.
"""
import argparse
from typing import Sequence

from xsamtools import view


def samtools_view(args: argparse.Namespace, extra_args: Sequence[str]):
    """
    A limited wrapper around "samtools view", but with functions to operate on drs and google cloud bucket keys.
    """
    print(f'samtools_cli_view: {extra_args}')
    view.samtools_view(extra_args)
