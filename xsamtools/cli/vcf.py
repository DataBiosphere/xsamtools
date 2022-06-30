"""
VCF file utilities
"""
import argparse
from typing import List

from xsamtools import vcf


def merge(args: argparse.Namespace, extra_args: List[str]):
    """
    Merge VCFs stored in google buckets pointed to by `input_keys`.
    Output to `output_key` in the same bucket.

    xsamtools vcf merge --bucket "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10" \\
    --inputs "a.vcf.gz,b.vcf.gz" \\
    --output "combined.vcf.gz"
    """
    inputs = args.inputs.split(",")
    vcf.combine(inputs, args.output, *extra_args)


def subsample(args: argparse.Namespace, extra_args: List[str]):
    """
    Subsample VCF a stored locally or in google bucket.
    """
    vcf.subsample(args.input, args.output, args.samples.split(","), *extra_args)


def stats(args: argparse.Namespace, extra_args: List[str]):
    """
    Statistics for VCF stored locally or in google bucket.
    """
    vcf.stats(args.input, *extra_args)
