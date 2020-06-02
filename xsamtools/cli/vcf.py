"""
VCF file utilities
"""
import argparse

from xsamtools import vcf
from xsamtools.cli import dispatch


vcf_cli = dispatch.group("vcf")


@vcf_cli.command("merge", arguments={
    "--inputs": dict(type=str,
                     required=True,
                     help="Input VCFs. These can be Google Storage objects if prefixed with 'gs://'"),
    "--output": dict(type=str,
                     required=True,
                     help="Output VCF. This can be a Google Storage object if prefixed with 'gs://'"),
})
def merge(args: argparse.Namespace):
    """
    Merge VCFs stored in google buckets pointed to by `input_keys`.
    Output to `output_key` in the same bucket.

    xsamtools vcf merge --bucket "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10" \\
    --inputs "a.vcf.gz,b.vcf.gz" \\
    --output "combined.vcf.gz"
    """
    inputs = args.inputs.split(",")
    vcf.combine(inputs, args.output)

@vcf_cli.command("subsample", arguments={
    "--input": dict(type=str,
                    required=True,
                    help="Input file. This can be a Google Storage object if prefixed with 'gs://'"),
    "--output": dict(type=str,
                     required=True,
                     help="Output VCF. This can be a Google Storage object if prefixed with 'gs://'"),
    "--samples": dict(type=str, required=True, help="Comma sepearted list of samples")
})
def subsample(args: argparse.Namespace):
    """
    Subsample VCF a stored locally or in google bucket.
    """
    vcf.subsample(args.input, args.output, args.samples.split(","))

@vcf_cli.command("stats", arguments={
    "--input": dict(type=str,
                    required=True,
                    help="Input file. This can be a Google Storage object if prefixed with 'gs://'"),
})
def stats(args: argparse.Namespace):
    """
    Statistics for VCF stored locally or in google bucket.
    """
    vcf.stats(args.input)
