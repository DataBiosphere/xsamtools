"""
VCF file utilities
"""
import argparse

from xsamtools import vcf


def merge(args: argparse.Namespace):
    """
    Merge VCFs stored in google buckets pointed to by `input_keys`.
    Output to `output_key` in the same bucket.

    xsamtools vcf merge --bucket "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10" \\
    --inputs "a.vcf.gz,b.vcf.gz" \\
    --output "combined.vcf.gz"
    """
    inputs = args.inputs.split(",")
    extra_args = []
    if args.force_samples:
        extra_args.append('--force-samples')
    if args.print_header:
        extra_args.append('--print-header')
    vcf.combine(inputs, args.output, *extra_args)


def subsample(args: argparse.Namespace):
    """
    Subsample VCF a stored locally or in google bucket.
    """
    vcf.subsample(args.input, args.output, args.samples.split(","))


def stats(args: argparse.Namespace):
    """
    Statistics for VCF stored locally or in google bucket.
    """
    vcf.stats(args.input)
