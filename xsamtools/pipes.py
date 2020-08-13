import os
import time
import multiprocessing
from uuid import uuid4
from contextlib import AbstractContextManager
from concurrent.futures import ProcessPoolExecutor, Future, as_completed

import gs_chunked_io as gscio

from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


class BlobReaderProcess(AbstractContextManager):
    def __init__(self, url, filepath=None, credentials_data=None):
        assert url.startswith("gs://") or url.startswith("drs://")
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.queue = multiprocessing.Manager().Queue()
        self.executor = ProcessPoolExecutor(max_workers=1)
        self.future = self.executor.submit(BlobReaderProcess.run, url, self.filepath, self.queue)
        self.future.add_done_callback(check_future_result)
        self._closed = False

    @staticmethod
    def run(url: str, filepath: str, queue: multiprocessing.Queue):
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
        with open(filepath, "wb") as fh:
            while True:
                data = bytearray(blob_reader.read(blob_reader.chunk_size))
                if not data:
                    break
                while data:
                    if not queue.empty() and queue.get_nowait():
                        return
                    try:
                        k = fh.write(data)
                        data = data[k:]
                    except BrokenPipeError:
                        time.sleep(1)

    def close(self):
        if not self._closed:
            self._closed = True
            self.queue.put("stop")
            self.executor.shutdown(wait=True)
            os.unlink(self.filepath)

    def __exit__(self, *args, **kwargs):
        self.close()

class BlobWriterProcess(AbstractContextManager):
    def __init__(self, bucket_name, key, filepath=None):
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.executor = ProcessPoolExecutor(max_workers=1)
        self.future = self.executor.submit(BlobWriterProcess.run, bucket_name, key, self.filepath)
        self.future.add_done_callback(check_future_result)
        self._closed = False

    @staticmethod
    def run(bucket_name, key, filepath):
        bucket = gs.get_client().bucket(bucket_name)
        with open(filepath, "rb") as fh:
            with gscio.Writer(key, bucket, threads=1) as blob_writer:
                while True:
                    data = fh.read(blob_writer.chunk_size)
                    if not data:
                        break
                    blob_writer.write(data)

    def close(self, timeout: int=300):
        if not self._closed:
            self._closed = True
            for _ in as_completed([self.future], timeout=300):
                pass
            self.executor.shutdown()
            os.unlink(self.filepath)

    def __exit__(self, *args, **kwargs):
        self.close()

def check_future_result(f: Future):
    try:
        f.result()
    except Exception:
        import traceback
        traceback.print_exc()
        os._exit(1)  # bail out without waiting around for forked processes
