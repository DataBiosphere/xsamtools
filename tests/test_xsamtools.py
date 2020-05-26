#!/usr/bin/env python
import io
import os
import unittest
from random import randint

from terra_notebook_utils import gs

from xsamtools import pipes


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


if __name__ == '__main__':
    unittest.main()
