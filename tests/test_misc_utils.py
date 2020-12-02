#!/usr/bin/env python
import os
import sys
import unittest
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import SuppressWarningsMixin  # noqa
from xsamtools.misc_utils import subprocess_w_stderr_run, SubprocessErrorStdError  # noqa

log = logging.getLogger(__name__)


class TestSubprocessStdErr(SuppressWarningsMixin, unittest.TestCase):
    def test_subprocess_prints_stderr(self):
        with self.subTest('Assert subprocess_w_stderr_run() raises SubprocessErrorStdError.'):
            with self.assertRaises(SubprocessErrorStdError) as e:
                subprocess_w_stderr_run(cmd='this-grand-command-wont-land-as-planned', check=True)
            self.assertEqual(e.exception.returncode, 127)

        with self.subTest('Assert subprocess_w_stderr_run() prints stderr when raising SubprocessErrorStdError.'):
            exc = ''
            try:
                subprocess_w_stderr_run(cmd='this-grand-command-wont-land-as-planned', check=True)
            except SubprocessErrorStdError:
                import traceback
                exc = traceback.format_exc()
            self.assertTrue('/bin/sh: 1: this-grand-command-wont-land-as-planned: not found' in exc, exc)

if __name__ == '__main__':
    unittest.main()
