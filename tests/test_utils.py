#!/usr/bin/env python
import os
import sys
import unittest
import logging

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests.infra import SuppressWarningsMixin  # noqa
from xsamtools.utils import run, XSamtoolsCalledProcessError  # noqa

log = logging.getLogger(__name__)


class TestSubprocessStdErr(SuppressWarningsMixin, unittest.TestCase):
    def test_subprocess_prints_stderr(self):
        with self.subTest('Assert xsamtools.utils.run() prints stderr when raising XSamtoolsCalledProcessError.'):
            exc = ''
            try:
                run(cmd=['this-grand-command-wont-land-as-planned'], shell=True)  # won't work without shell=True ?
            except XSamtoolsCalledProcessError:
                import traceback
                exc = traceback.format_exc()
            self.assertTrue('/bin/sh: 1: this-grand-command-wont-land-as-planned: not found' in exc, exc)
            self.assertTrue('failed with return code: 127' in exc, exc)

if __name__ == '__main__':
    unittest.main()
