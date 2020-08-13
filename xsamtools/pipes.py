import os
import time
from uuid import uuid4
from contextlib import AbstractContextManager
from multiprocessing import Process

import gs_chunked_io as gscio

from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


class BlobReaderProcess(AbstractContextManager):
    def __init__(self, url, filepath=None, credentials_data=None):
        assert url.startswith("gs://") or url.startswith("drs://")
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.proc = Process(target=self.run, args=(url, self.filepath))
        self.proc.start()
        self._closed = False

    def run(self, url, filepath):
        if url.startswith("gs://"):
            bucket_name, key = url[5:].split("/", 1)
            client = gs.get_client()
            bucket = client.bucket(bucket_name)
        elif url.startswith("drs://"):
            client, info = drs.resolve_drs_for_gs_storage(url)
            drs.enable_requester_pays()
            bucket = client.bucket(info.bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
            key = info.key
        else:
            raise ValueError(f"Unsupported schema for url: {url}")
        blob = bucket.get_blob(key)
        blob_reader = gscio.Reader(blob, threads=1)
        fd = os.open(filepath, os.O_WRONLY)
        while True:
            data = bytearray(blob_reader.read(blob_reader.chunk_size))
            if not data:
                break
            while data:
                try:
                    k = os.write(fd, data)
                    data = data[k:]
                except BrokenPipeError:
                    time.sleep(1)

    def close(self):
        if not self._closed:
            self._closed = True
            self.proc.join(1)
            os.unlink(self.filepath)
            if not self.proc.exitcode:
                self.proc.terminate()

    def __exit__(self, *args, **kwargs):
        self.close()


class BlobWriterProcess(AbstractContextManager):
    def __init__(self, bucket_name, key, filepath=None):
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.proc = Process(target=self.run, args=(bucket_name, key, self.filepath))
        self.proc.start()
        self._closed = False

    def run(self, bucket_name, key, filepath):
        bucket = gs.get_client().bucket(bucket_name)
        with gscio.Writer(key, bucket, threads=1) as blob_writer:
            with open(filepath, "rb") as fh:
                while True:
                    data = fh.read(blob_writer.chunk_size)
                    if not data:
                        break
                    blob_writer.write(data)

    def close(self, timeout=300):
        if not self._closed:
            self._closed = True
            self.proc.join(timeout)  # TODO: Consider inter-process communication to know when it's safe to close
            os.unlink(self.filepath)
            if not self.proc.exitcode:
                self.proc.terminate()

    def __exit__(self, *args, **kwargs):
        self.close()
