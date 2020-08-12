"""
CRAM file utilities
"""
import os
import argparse
import datetime

from xsamtools import cram
from xsamtools.cli import dispatch


cram_cli = dispatch.group("cram")


@cram_cli.command("view", arguments={
    "--cram": dict(type=str,
                   required=True,
                   help="Input cram file. This can be a Google Storage object if prefixed with 'gs://'."),
    "--crai": dict(type=str,
                   required=False,
                   help="Input crai file. This can be a Google Storage object if prefixed with 'gs://'.  "
                        "If not specified, one will be generated for you (this may take a long time)."),
    # TODO: add an argument to intake a BED file, as that's the more rational use-case.
    "--regions": dict(type=str,
                      required=False,
                      default=None,
                      help="A comma-delimited list of regions of sequence in the input cram file to subset as the "
                           "output CRAM.  If unspecified, the entire file will be copied over."),
    "-C": dict(action='store_true',
               required=False,
               help="Write the output file in CRAM format."),
    # TODO: Allow this to be a google key.
    "--output": dict(type=str,
                     required=False,
                     help="A local output file path for the generated cram file.  "
                          "If unspecified, this will be created wherever the input cram file was located.")
})
def view(args: argparse.Namespace):
    """
    A limited wrapper around "samtools view", but with functions to operate on google cloud bucket keys.
    """
    extension = 'cram' if args.C else 'sam'
    if not args.output:
        time_stamp = str(datetime.datetime.now()).split('.')[0].replace(':', '').replace(' ', '-')
        # NOTE: schema output (gs:// or file://) is preserved
        args.output = os.path.abspath(f'{time_stamp}.output.{extension}')
    if '://' in args.output and not args.output.startswith('file://'):
        raise NotImplementedError(f'Schema not yet supported: {args.output}')
    cram.view(cram=args.cram, crai=args.crai, regions=args.regions, output=args.output, cram_format=args.C)
