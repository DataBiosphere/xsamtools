"""
CRAM file utilities.
"""
import argparse

from xsamtools import cram
from xsamtools.cli import dispatch


cram_cli = dispatch.group("cram")


@cram_cli.command("view", arguments={
    "--cram": dict(type=str,
                   required=True,
                   help="Input cram file. This can be a Google Storage object if prefixed with 'gs://'."),
    "--crai": dict(type=str,
                   required=False,
                   help="Input crai file. This can be a Google Storage file (e.g. gs://bucket/key) or a local file.  "
                        "If not specified, one will be generated for you (this may take a long time)."),
    # TODO: add an argument to intake a BED file.
    "--regions": dict(type=str,
                      required=False,
                      default=None,
                      help="A comma-delimited list of regions of sequence in the input cram file to subset as the "
                           "output CRAM.  For example, something like: 'ch1,ch2' or 'chromsome_1:10000,chromosome2'."),
    "-C": dict(action='store_true',
               required=False,
               help="Write the output file in CRAM format."),
    # TODO: Allow this to be a google key.
    "--output": dict(type=str,
                     required=False,
                     default=None,
                     help="A local output file path for the generated cram file.")
})
def view(args: argparse.Namespace):
    """
    A limited wrapper around "samtools view", but with functions to operate on google cloud bucket keys.
    """
    cram.view(cram=args.cram, crai=args.crai, regions=args.regions, output=args.output, cram_format=args.C)
