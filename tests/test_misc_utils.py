#!/usr/bin/env python
import os
import sys
import warnings
import unittest
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from xsamtools.misc_utils import subprocess_w_stderr_run, SubprocessErrorStdError

log = logging.getLogger(__name__)


class TestCram(unittest.TestCase):
    def setUp(self):
        # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
        warnings.filterwarnings('ignore', 'Your application has authenticated using end user credentials')
        # Suppress unclosed socket warnings
        warnings.simplefilter('ignore', ResourceWarning)

    def test_subprocess_prints_stderr(self):
        with self.subTest('Assert subprocess_w_stderr_run() raises SubprocessErrorStdError.'):
            with self.assertRaises(SubprocessErrorStdError) as e:
                subprocess_w_stderr_run(cmd='this-grand-command-wont-land-as-planned', check=True)
            self.assertEqual(e.exception.returncode, 127)

        with self.subTest('Assert subprocess_w_stderr_run() prints stderr when raising SubprocessErrorStdError.'):
            exc = ''
            try:
                subprocess_w_stderr_run(cmd='this-grand-command-wont-land-as-planned', check=True)
            except:
                import traceback
                exc = traceback.format_exc()
            self.assertTrue('/bin/sh: 1: this-grand-command-wont-land-as-planned: not found' in exc, exc)


if __name__ == '__main__':
    unittest.main()
