#!/usr/bin/env python
"""
Subsample VCF a stored locally or in google bucket.
"""
import argparse

from xsamtools import vcf

parser = argparse.ArgumentParser()
parser.add_argument("input",
                    type=str,
                    help="Input file. This can be a Google Storage path if prefixed with 'gs://'")
parser.add_argument("--output",
                    "-o",
                    type=str,
                    help="Output file. This can be a Google Storage path if prefixed with 'gs://'")
parser.add_argument("--samples",
                    help="Filepath of samples. See 'bcftools view' for usage")
args = parser.parse_args()

keys = args.input_keys.split(",")
vcf.combine(args.bucket, keys, args.bucket, args.output_key)
