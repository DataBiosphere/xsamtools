#!/usr/bin/env python
import io
import os
import sys
import unittest
from uuid import uuid4

# WORKSPACE_NAME and GOOGLE_PROJECT are needed for tnu.drs.enable_requester_pays()
WORKSPACE_NAME = "terra-notebook-utils-tests"
GOOGLE_PROJECT = "firecloud-cgl"
WORKSPACE_BUCKET = "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['WORKSPACE_NAME'] = WORKSPACE_NAME
os.environ['GOOGLE_PROJECT'] = GOOGLE_PROJECT
os.environ['WORKSPACE_BUCKET'] = WORKSPACE_BUCKET

from terra_notebook_utils import gs  # noqa

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import gs_utils  # noqa
from tests.infra import SuppressWarningsMixin  # noqa


class TestGSUtils(SuppressWarningsMixin, unittest.TestCase):
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
