"""
CRAM file utilities
"""
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
    # TODO: add an argument to intake a BED file, as that's the more rational use-case
    "--regions": dict(type=str,
                      required=False,
                      default=None,
                      help="A comma-delimited list of regions of sequence in the input cram file to subset as the "
                           "output CRAM.  If unspecified, the entire file will be copied over."),
    "-C": dict(action='store_true',
               required=False,
               help="Write the output file in CRAM format."),
    "--output": dict(type=str,
                     required=False,
                     help="Output file. This can be a Google Storage object if prefixed with 'gs://'.  "
                          "If unspecified, this will be created wherever the input cram file was located.")
})
def view(args: argparse.Namespace):
    """
    A limited wrapper around "samtools view", but with functions to operate on google cloud bucket keys.
    """
    if not args.output:
        time_stamp = str(datetime.datetime.now()).split('.')[0].replace(':', '').replace(' ', '-')
        args.output = f'{args.cram}.{time_stamp}.output.cram'  # schema output, i.e. gs:// or file//:, is preserved
    cram.view(cram=args.cram, crai=args.crai, regions=args.regions, output=args.output, cram_format=args.C)
    # print(args.cram, args.crai, args.C, args.regions, args.output)
