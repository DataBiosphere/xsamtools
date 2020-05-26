#!/usr/bin/env python
import io
import os
import sys
import typing
import unittest
from random import randint

from terra_notebook_utils import gs
from terra_notebook_utils.vcf import VCFInfo

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import pipes, samtools  # noqa
samtools.paths['bcftools'] = "build/bcftools/bcftools"
from xsamtools import vcf  # noqa


WORKSPACE_BUCKET = "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"


class TestXsamtoolsNamedPipes(unittest.TestCase):
    def test_blob_reader(self):
        key = "test_blob_reader_obj"
        data = os.urandom(1024 * 1024 * 50)
        with io.BytesIO(data) as fh:
            gs.get_client().bucket(WORKSPACE_BUCKET).blob(key).upload_from_file(fh)
        with pipes.BlobReaderProcess(WORKSPACE_BUCKET, key) as reader:
            in_data = bytearray()
            with open(reader.filepath, "rb") as fh:
                while True:
                    d = fh.read(randint(1024, 1024 * 1024))
                    if d:
                        in_data += d
                    else:
                        break
        self.assertEqual(data, in_data)

    def test_blob_writer(self):
        key = "test_blob_writer_obj"
        data = os.urandom(1024 * 1024 * 50)
        with pipes.BlobWriterProcess(WORKSPACE_BUCKET, key) as writer:
            with open(writer.filepath, "wb") as fh:
                out_data = bytearray(data)
                while True:
                    chunk_size = randint(1024, 1024 * 1024)
                    to_write = out_data[:chunk_size]
                    out_data = out_data[chunk_size:]
                    if to_write:
                        fh.write(to_write)
                    else:
                        break

        with io.BytesIO() as fh:
            gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key).download_to_file(fh)
            self.assertEqual(fh.getvalue(), data)


class TestXsamtools(unittest.TestCase):
    def test_combine(self):
        keys = ["test_vcfs/a.vcf.gz", "test_vcfs/b.vcf.gz"]
        output_key = "test_bcftools_combined.vcf.gz"
        vcf.combine(WORKSPACE_BUCKET, keys, WORKSPACE_BUCKET, output_key)
        blob = gs.get_client().bucket(WORKSPACE_BUCKET).blob(output_key)
        info = VCFInfo.with_blob(blob)
        self._assert_vcf_info(info)

    def _assert_vcf_info(self, info):
        root = os.path.dirname(__file__)
        expected_info = VCFInfo.with_file(os.path.join(root, "fixtures/expected.vcf.gz"))
        self.assertTrue(self._headers_equal(info.header, expected_info.header))
        for name in VCFInfo.columns:
            self.assertEqual(getattr(info, name), getattr(expected_info, name))

    def _headers_equal(self, header_a: typing.List[str], header_b: typing.List[str]) -> bool:
        for a, b in zip(header_a, header_b):
            if "bcftools" in a:
                continue
            elif a != b:
                return False
        return True


if __name__ == '__main__':
    unittest.main()
