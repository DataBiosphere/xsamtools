#!/usr/bin/env python
import io
import os
import subprocess
import sys
import unittest
import unittest.mock
from pathlib import Path
from subprocess import CalledProcessError
from typing import List

# WORKSPACE_NAME and GOOGLE_PROJECT are needed for tnu.drs.enable_requester_pays()
WORKSPACE_NAME = "terra-notebook-utils-tests"
GOOGLE_PROJECT = "firecloud-cgl"
WORKSPACE_BUCKET = "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['WORKSPACE_NAME'] = WORKSPACE_NAME
os.environ['GOOGLE_PROJECT'] = GOOGLE_PROJECT
os.environ['WORKSPACE_BUCKET'] = WORKSPACE_BUCKET

from terra_notebook_utils import gs  # noqa
from terra_notebook_utils.vcf import VCFInfo  # noqa

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import samtools  # noqa
samtools.paths['bcftools'] = "build/bcftools/bcftools"
from xsamtools import vcf  # noqa
from tests.infra import SuppressWarningsMixin  # noqa


class TestXsamtools(SuppressWarningsMixin, unittest.TestCase):
    def test_combine(self):
        with self.subTest("cloud locations"):
            inputs = [f"gs://{WORKSPACE_BUCKET}/test_vcfs/{n}.vcf.gz" for n in "ab"]
            output = f"gs://{WORKSPACE_BUCKET}/test_bcftools_combined.vcf.gz"
            vcf.combine(inputs, output)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).blob("test_bcftools_combined.vcf.gz")
            info = VCFInfo.with_blob(blob)
            self._assert_vcf_info(info)
        with self.subTest("local paths"):
            inputs = ["tests/fixtures/a.vcf.gz", "tests/fixtures/b.vcf.gz"]
            output = "test_bcftools_combined.vcf.gz"
            vcf.combine(inputs, output)
            info = VCFInfo.with_file(output)
            self._assert_vcf_info(info)
        with self.subTest("bad input"):
            with self.assertRaises(CalledProcessError):
                inputs = [f"gs://{WORKSPACE_BUCKET}/test_vcfs/{n}.vcf.gz" for n in "ab"]
                inputs.append(f"gs://{WORKSPACE_BUCKET}/test_vcfs/broken.vcf.gz")
                output = "test_bcftools_combined.vcf.gz"
                vcf.combine(inputs, output)

    def test_cli_vcf_merge(self):
        inputs = ["tests/fixtures/a.vcf.gz", "tests/fixtures/b.vcf.gz"]
        output = "test_bcftools_combined.vcf.gz"
        # Test with some arguments that get passed on to bcftools
        vcf.combine(inputs, output, '--force-samples', '--print-header')
        self.assertTrue(Path(output).stat().st_size > 0)

    def test_cli_subsample(self):
        samples = ["NWD994242", "NWD637453"]
        output_key = "test_bcftools_subsampled.vcf.gz"
        src_path = "tests/fixtures/expected.vcf.gz"
        dst_path = output_key
        # Test with some arguments that get passed on to bcftools
        vcf.subsample(src_path, dst_path, samples, '--no-header', '--no-version')
        self.assertGreater(Path(dst_path).stat().st_size, 0)

    def test_cli_stats(self):
        src_path = "tests/fixtures/expected.vcf.gz"
        # Test with some arguments that get passed on to bcftools
        result = []
        # save run to avoid infinite mock loop
        run_function = subprocess.run

        def capturing_run(*args, **kwargs):
            run = run_function(*args, **kwargs, capture_output=True)
            result.append(run)
            return run
        with unittest.mock.patch('subprocess.run', new=capturing_run):
            vcf.stats(src_path, '--1st-allele-only')
        # Unpacking ensures single call was made to subprocess.run
        result, =  result
        expected = b'# This file was produced by bcftools '
        self.assertTrue(result.stdout.startswith(expected))

    def test_preset_args(self):
        # Test with some arguments that get passed on to bcftools
        with self.assertRaises(ValueError) as cm:
            vcf.subsample("baz", "bar", ["foo"], '-S')
        self.assertTrue(cm.exception.args[0].startswith("The following args cannot be supplied manually"))
        with self.assertRaises(ValueError) as cm:
            vcf.stats("baz", '--threads')
        self.assertTrue(cm.exception.args[0].startswith("The following args cannot be supplied manually"))
        with self.assertRaises(ValueError) as cm:
            vcf.combine("baz", "foo", '-O', '--no-index')
        self.assertTrue(cm.exception.args[0].startswith("The following args cannot be supplied manually"))

    def test_subsample(self):
        samples = ["NWD994242", "NWD637453"]
        with self.subTest("test cloud locations"):
            output_key = "test_bcftools_subsampled.vcf.gz"
            src_path = f"gs://{WORKSPACE_BUCKET}/test_vcfs/a.vcf.gz"
            dst_path = f"gs://{WORKSPACE_BUCKET}/{output_key}"
            vcf.subsample(src_path, dst_path, samples)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).blob(output_key)
            info = VCFInfo.with_blob(blob)
            self.assertListEqual(info.samples, samples)
        with self.subTest("test local paths"):
            output_key = "test_bcftools_subsampled.vcf.gz"
            src_path = "tests/fixtures/expected.vcf.gz"
            dst_path = output_key
            vcf.subsample(src_path, dst_path, samples)
            info = VCFInfo.with_file(output_key)
            self.assertListEqual(info.samples, samples)

    def _assert_vcf_info(self, info):
        root = os.path.dirname(__file__)
        expected_info = VCFInfo.with_file(os.path.join(root, "fixtures/expected.vcf.gz"))
        self.assertTrue(self._headers_equal(info.header, expected_info.header))
        for name in VCFInfo.columns:
            self.assertEqual(getattr(info, name), getattr(expected_info, name))

    def _headers_equal(self, header_a: List[str], header_b: List[str]) -> bool:
        for a, b in zip(header_a, header_b):
            if "bcftools" in a:
                continue
            elif a != b:
                return False
        return True

if __name__ == '__main__':
    unittest.main()
