#!/usr/bin/env python
import os
import sys
import warnings
import unittest

from urllib.request import urlretrieve

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools import cram  # noqa

WORKSPACE_BUCKET = 'fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10'


class TestCram(unittest.TestCase):
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings('ignore', 'Your application has authenticated using end user credentials')
        # Suppress unclosed socket warnings
        warnings.simplefilter('ignore', ResourceWarning)

    @classmethod
    def setUpClass(cls) -> None:
        # gs://lons-test/ce#5b.cram
        cls.test_cram = cram.download_gs(gs_path='gs://lons-test/ce#5b.cram',
                                         output_filename=os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram'))
        # gs://lons-test/ce#5b.crai
        cls.test_crai = cram.download_gs(gs_path='gs://lons-test/ce#5b.cram.crai',
                                         output_filename=os.path.join(pkg_root, 'tests/fixtures/ce#5b.crai'))

        # # gs://lons-test/ce#5b.cram
        # cls.test_cram = os.path.join(pkg_root, 'tests/fixtures/ce#5b.cram')
        # urlretrieve('https://storage.googleapis.com/lons-test/ce#5b.cram', cls.test_cram)
        # # gs://lons-test/ce#5b.crai
        # cls.test_crai = os.path.join(pkg_root, 'tests/fixtures/ce#5b.crai')
        # urlretrieve('https://storage.googleapis.com/lons-test/ce#5b.crai', cls.test_crai)

    @classmethod
    def tearDownClass(cls) -> None:
        for file in [cls.test_cram, cls.test_crai]:
            if os.path.exists(file):
                os.remove(file)

    def test_cram_view_api(self):
        with self.subTest('xsamtools view -C'):
            cram_output = cram.view(cram=self.test_cram, crai=self.test_crai, regions=None, cram_format=True, output='')
            self.assertTrue(os.path.exists(cram_output))
            self.assertEqual(os.stat(cram_output).st_size, os.stat(self.test_cram).st_size)


if __name__ == '__main__':
    unittest.main()
