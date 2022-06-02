import argparse
import subprocess
from textwrap import dedent

from xsamtools import samtools
from xsamtools.cli.cram import view
from xsamtools.cli.vcf import merge, subsample, stats


def add_cram_subparser(subparsers):
    cram_parser = subparsers.add_parser('cram')
    # The api here is somewhat redundant, cram only has a single sub-command/parser
    cram_subparsers = cram_parser.add_subparsers()
    view_parser = cram_subparsers.add_parser('view', description='A limited wrapper around "samtools view", but with '
                                                                 'functions to operate on google cloud bucket keys.')
    view_parser.add_argument("--cram", type=str, required=True,
                             help="Input cram file. This can be a Google Storage object if prefixed with 'gs://'.")
    view_parser.add_argument("--crai", type=str, required=False,
                             help="Input crai file. This can be a Google Storage file (e.g. gs://bucket/key) or a "
                                  "local file. If not specified, one will be generated for you (this may take a long "
                                  "time).")
    # TODO: add an argument to intake a BED file.
    view_parser.add_argument("--regions", type=str, required=False, default=None,
                             help="A comma-delimited list of regions of sequence in the input cram file to subset as "
                                  "the output CRAM.  For example, something like: 'ch1,ch2' or "
                                  "'chromsome_1:10000,chromosome2'.")
    view_parser.add_argument("-C", action='store_true', required=False,
                             help="Write the output file in CRAM format.")
    # TODO: Allow this to be a google key.
    view_parser.add_argument("--output", type=str, required=False, default=None,
                             help="A local output file path for the generated cram file.")
    view_parser.set_defaults(func=view)


def merge_options():
    # Help returns non-zero exit status for some reason
    result = subprocess.run([samtools.paths['bcftools'], "merge", "--help"], capture_output=True, check=False)
    help = result.stderr.decode()
    _, options = help.split('Options:')
    return "bcftools merge arguments:" + options


def add_vcf_subparser(subparsers):
    vcf_parser = subparsers.add_parser('vcf')
    vcf_subparsers = vcf_parser.add_subparsers()
    description = dedent("""
        Merge VCFs stored in google buckets pointed to by `input_keys`.
        Output to `output_key` in the same bucket. Additional arguments will be passed
        on to bcftools merge.

        xsamtools vcf merge --bucket "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10" \\
        --inputs "a.vcf.gz,b.vcf.gz" \\
        --output "combined.vcf.gz""").strip()
    merge_parser = vcf_subparsers.add_parser('merge',
                                             description=description,
                                             formatter_class=argparse.RawDescriptionHelpFormatter,
                                             epilog=merge_options())
    merge_parser.add_argument("--inputs", type=str, required=True,
                              help="Input VCFs. These can be Google Storage objects if prefixed with 'gs://'")
    merge_parser.add_argument("--output", type=str, required=True,
                              help="Output VCF. This can be a Google Storage object if prefixed with 'gs://'")
    merge_parser.set_defaults(func=merge)

    subsample_parser = vcf_subparsers.add_parser('subsample', description=("Subsample VCF a stored locally "
                                                                           "or in google bucket. Additional "
                                                                           "arguments will be passed on to bcftools"))
    subsample_parser.add_argument("--input", type=str, required=True,
                                  help="Input file. This can be a Google Storage object if prefixed with 'gs://'")
    subsample_parser.add_argument("--output", type=str, required=True,
                                  help="Output VCF. This can be a Google Storage object if prefixed with 'gs://'")
    subsample_parser.add_argument("--samples", type=str, required=True, help="Comma seperated list of samples")
    subsample_parser.set_defaults(func=subsample)

    stats_parser = vcf_subparsers.add_parser('stats', description=("Statistics for VCF stored locally "
                                                                   "or in google bucket. Additional arguments "
                                                                   "will be passed on to bcftools"))
    stats_parser.add_argument("--input", type=str, required=True,
                              help="Input file. This can be a Google Storage object if prefixed with 'gs://'")
    stats_parser.set_defaults(func=stats)


def main(args):
    parser = argparse.ArgumentParser(description='xsamtools is awesome.')
    subparsers = parser.add_subparsers()
    add_cram_subparser(subparsers)
    add_vcf_subparser(subparsers)
    args, extra_args = parser.parse_known_args(args)
    args.func(args, extra_args)
