"""
CRAM file utilities.
"""
import argparse
from typing import Sequence

from xsamtools import cram


def view(args: argparse.Namespace, extra_args: Sequence[str]):
    """
    A limited wrapper around "samtools view", but with functions to operate on google cloud bucket keys.
    """
    cram.view(cram=args.cram, crai=args.crai, regions=args.regions, output=args.output, cram_format=args.C)
