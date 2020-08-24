#!/usr/bin/env python
import io
import os
import sys
import typing
import warnings
import unittest
from uuid import uuid4
from random import randint
from contextlib import closing
from concurrent.futures import ProcessPoolExecutor

# WORKSPACE_NAME and GOOGLE_PROJECT are needed for tnu.drs.enable_requester_pays()
WORKSPACE_NAME = "terra-notebook-utils-tests"
GOOGLE_PROJECT = "firecloud-cgl"
WORKSPACE_BUCKET = "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['WORKSPACE_NAME'] = WORKSPACE_NAME
os.environ['GOOGLE_PROJECT'] = GOOGLE_PROJECT
os.environ['WORKSPACE_BUCKET'] = WORKSPACE_BUCKET

from google.cloud.exceptions import NotFound  # noqa
from terra_notebook_utils import gs  # noqa
from terra_notebook_utils.vcf import VCFInfo  # noqa

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

warnings.simplefilter("ignore", UserWarning)
from xsamtools import pipes, samtools, gs_utils  # noqa
samtools.paths['bcftools'] = "build/bcftools/bcftools"
from xsamtools import vcf  # noqa


class TestXsamtoolsNamedPipes(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", UserWarning)
        warnings.simplefilter("ignore", ResourceWarning)
        self.executor = ProcessPoolExecutor(max_workers=4)

    def tearDown(self):
        self.executor.shutdown()

    def test_blob_reader(self):
        with self.subTest("gs url"):
            key = "test_blob_reader_obj"
            data = os.urandom(1024 * 1024 * 50)
            with io.BytesIO(data) as fh:
                gs.get_client().bucket(WORKSPACE_BUCKET).blob(key).upload_from_file(fh)
            url = f"gs://{WORKSPACE_BUCKET}/{key}"
            with pipes.BlobReaderProcess(url, self.executor) as reader:
                handle, first_byte = reader.get_handle()
                in_data = bytearray(first_byte)
                with closing(handle):
                    while True:
                        d = handle.read(randint(1024, 1024 * 1024))
                        if d:
                            in_data += d
                        else:
                            break
            self.assertEqual(data, in_data)
        with self.subTest("drs url"):
            url = "drs://dg.4503/57f58130-2d66-4d46-9b2b-539f7e6c2080"
            expected_data = (b'\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\xd7$\x8d][\xcf-9q}\xcf\xaf@'
                             b'\xf0\x8a"\xdb\xe5\xab\x94\x89\x04\x01\x02\x84DQ \x08\x9eF\x08\x86\x8b4\xcc\xa0a\x84\x94'
                             b'\x7f\x1f\xf7\xe9\xdd\xb6\xb7\xd7r\xd5\xf7=\x9e\xe3\xb5\xbb\xdbU.W\xad*\x97\xbf\xf7\xbd?'
                             b'\xfe\xe5\xcb/\xfe\xf8\xf57\x7f\xfd\xdd\xb7\x9f\xfd\xfa\xdf~\xf2\x8f\xf8\xcf\xe1\x9f\xbe'
                             b'\xf7\xbd\x9f\xfc\xec\x17\xbf\xfa\xf1\xff|\xf6/?\xfb\xd1g\xff\xfd\x83_\xfe\xf2\xfb?\xfa'
                             b'\xe2\xef\xbf\xff\xe6/\x7f\xfb\xf6/_\x7f\xf5\xd9w\x7f\xf0\xe5\x97\xdf\xe9\xa8o\xbf\xf8'
                             b'\xe6\xef\xdf\xf9\xdb\xef\xfe\xfe\xf7/\xfe\xf0\xdd\x7f\xed\x98\xdf\x7f\xfd\xd5\xb7\x7f'
                             b'\xf9\xd3\'\xcc\xef\xff\xfc\x8d\xff\xfe\x97_|\xf5\xa7o\xff\xfcY\x88')
            with pipes.BlobReaderProcess(url, self.executor) as reader:
                handle, first_byte = reader.get_handle()
                data = bytearray(first_byte)
                with closing(handle):
                    data += handle.read(len(expected_data) - len(data))
            self.assertEqual(data[:len(expected_data)], expected_data)

    def test_blob_writer(self):
        key = "test_blob_writer_obj"
        data = os.urandom(1024 * 1024 * 50)
        with pipes.BlobWriterProcess(WORKSPACE_BUCKET, key, self.executor) as writer:
            with writer.get_handle() as handle:
                out_data = bytearray(data)
                while True:
                    chunk_size = randint(1024, 1024 * 1024)
                    to_write = out_data[:chunk_size]
                    out_data = out_data[chunk_size:]
                    if to_write:
                        handle.write(to_write)
                    else:
                        break

        with io.BytesIO() as fh:
            gs.get_client().bucket(WORKSPACE_BUCKET).get_blob(key).download_to_file(fh)
            self.assertEqual(fh.getvalue(), data)


class TestXsamtools(unittest.TestCase):
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
        # Suppress unclosed socket warnings
        warnings.simplefilter("ignore", ResourceWarning)

    def test_combine(self):
        with self.subTest("test cloud locations"):
            inputs = [f"gs://{WORKSPACE_BUCKET}/test_vcfs/{n}.vcf.gz" for n in "ab"]
            output = "test_bcftools_combined.vcf.gz"
            vcf.combine(inputs, output)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).blob(output)
            info = VCFInfo.with_blob(blob)
            self._assert_vcf_info(info)
        with self.subTest("test local paths"):
            inputs = ["tests/fixtures/a.vcf.gz", "tests/fixtures/b.vcf.gz"]
            output = "test_bcftools_combined.vcf.gz"
            vcf.combine(inputs, output)
            blob = gs.get_client().bucket(WORKSPACE_BUCKET).blob(output)
            info = VCFInfo.with_blob(blob)
            self._assert_vcf_info(info)

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

    def _headers_equal(self, header_a: typing.List[str], header_b: typing.List[str]) -> bool:
        for a, b in zip(header_a, header_b):
            if "bcftools" in a:
                continue
            elif a != b:
                return False
        return True

class TestGSUtils(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)

    def test_blob_for_url(self):
        bucket_name = WORKSPACE_BUCKET
        key = f"{uuid4()}"
        url = f"gs://{bucket_name}/{key}"
        gs.get_client().bucket(WORKSPACE_BUCKET).blob(key).upload_from_file(io.BytesIO(b"0"))
        self.assertIsNotNone(gs_utils._blob_for_url(url))

    def test_read_access(self):
        bucket_name = WORKSPACE_BUCKET
        key = f"{uuid4()}"
        gs.get_client().bucket(WORKSPACE_BUCKET).blob(key).upload_from_file(io.BytesIO(b"0"))
        url = f"gs://{bucket_name}/{key}"
        self.assertTrue(gs_utils._read_access(url))
        url = f"gs://{bucket_name}/bogus-key"
        gs_utils._blob_for_url(url)
        self.assertFalse(gs_utils._read_access(url))

    def test_write_access(self):
        tests = [(f"bogus-bucket-{uuid4()}", False),  # fake bucket
                 ("fc-fe788689-0e09-4797-9e68-d4f78d8daa59", False),  # real bucket, no access
                 (WORKSPACE_BUCKET, True)]
        for bucket_name, expected_access in tests:
            self.assertEqual(expected_access, gs_utils._write_access(bucket_name))

if __name__ == '__main__':
    unittest.main()
