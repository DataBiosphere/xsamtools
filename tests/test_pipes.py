#!/usr/bin/env python
import io
import os
import sys
import unittest
from random import randint
from contextlib import closing
from concurrent.futures import ProcessPoolExecutor
from typing import IO

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

from xsamtools import pipes  # noqa
from tests.infra import SuppressWarningsMixin  # noqa


class PIPETestException(Exception):
    pass

class TestPipe(SuppressWarningsMixin, pipes.FIFOPipeProcess):
    def __init__(self, *args, should_open=True, **kwargs):
        self.should_open = should_open
        super().__init__(*args,
                         subprocess_handle_timeout_seconds=2,
                         handle_timeout_seconds=2,
                         **kwargs)

    def run(self, fh: IO):
        if self.should_open:
            if "wb" == self.mode[:2]:
                fh.write(b"smurfs")
            elif "rb" == self.mode[:2]:
                assert b"squids" == fh.read(20)
            else:
                raise Exception("something got misconfigured")
        else:
            raise PIPETestException()

class TestXsamtoolsNamedPipes(SuppressWarningsMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.executor = ProcessPoolExecutor(max_workers=4)

    def tearDown(self):
        self.executor.shutdown()

    def test_fifo_pipe_process(self):
        with self.subTest("read pipe opens"):
            p = TestPipe(self.executor, mode="wb->rb", should_open=True)
            with p.get_handle() as fh:
                self.assertEqual(b"smurfs", fh.read())
            p.close()

        with self.subTest("write pipe opens"):
            p = TestPipe(self.executor, mode="rb<-wb", should_open=True)
            with p.get_handle() as fh:
                fh.write(b"squids")
            p.close()

        with self.subTest("read pipe raises"):
            with self.assertRaises(PIPETestException):
                with TestPipe(self.executor, mode="wb->rb", should_open=False):
                    pass

        with self.subTest("write pipe raises"):
            with self.assertRaises(PIPETestException):
                with TestPipe(self.executor, mode="rb<-wb", should_open=False):
                    pass

        with self.subTest("read pipe deadlocks"):
            with self.assertRaises(pipes.FIFOBrokenPipeException):
                p = TestPipe(self.executor, mode="wb->rb", should_open=True)
                p.close()

        with self.subTest("write pipe deadlocks"):
            with self.assertRaises(pipes.FIFOBrokenPipeException):
                p = TestPipe(self.executor, mode="rb<-wb", should_open=True)
                p.close()

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

if __name__ == '__main__':
    unittest.main()
