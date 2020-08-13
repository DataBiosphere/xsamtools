import os
import time
import multiprocessing
from uuid import uuid4
from contextlib import AbstractContextManager
from concurrent.futures import ProcessPoolExecutor, Future, as_completed
from typing import Tuple, BinaryIO, Optional

import gs_chunked_io as gscio

from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


class BlobReaderProcess(AbstractContextManager):
    def __init__(self, url: str, filepath: str=None):
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
        with open(filepath, "wb") as fh:
            with gscio.Reader(blob, threads=1) as blob_reader:
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

    def get_handle(self) -> Tuple[BinaryIO, bytes]:
        """
        Get readable handle while avoiding FIFO deadlocks.
        See: https://stackoverflow.com/questions/5782279/why-does-a-read-only-open-of-a-named-pipe-block
        """
        return _get_fifo_read_handle(self.filepath)

    def close(self):
        if not self._closed:
            self._closed = True
            self.queue.put("stop")
            self.executor.shutdown(wait=True)
            os.unlink(self.filepath)

    def __exit__(self, *args, **kwargs):
        self.close()

class BlobWriterProcess(AbstractContextManager):
    def __init__(self, bucket_name: str, key: str, filepath: Optional[str]=None):
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.executor = ProcessPoolExecutor(max_workers=1)
        self.future = self.executor.submit(BlobWriterProcess.run, bucket_name, key, self.filepath)
        self.future.add_done_callback(check_future_result)
        self._closed = False

    @staticmethod
    def run(bucket_name: str, key: str, filepath: str):
        bucket = gs.get_client().bucket(bucket_name)
        with open(filepath, "rb") as fh:
            with gscio.Writer(key, bucket, threads=1) as blob_writer:
                while True:
                    data = fh.read(blob_writer.chunk_size)
                    if not data:
                        break
                    blob_writer.write(data)

    def get_handle(self) -> BinaryIO:
        """
        Get writable handle while avoiding FIFO deadlocks.
        See: https://stackoverflow.com/questions/5782279/why-does-a-read-only-open-of-a-named-pipe-block
        """
        return _get_fifo_write_handle(self.filepath)

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

def _get_fifo_read_handle(filepath: str, timeout: int=10) -> Tuple[BinaryIO, bytes]:
    """
    Check that FIFO at `filepath` is readable and return high level file handle.
    If FIFO is not readable after `timeout`, raise `OSError`.
    """
    read_fd = os.open(filepath, os.O_RDONLY | os.O_NONBLOCK)
    for _ in range(timeout):
        try:
            first_byte = os.read(read_fd, 1)
            if first_byte:
                break
        except BlockingIOError:
            pass
        time.sleep(1)
    else:
        raise OSError("pipe failed to open")
    fh = open(filepath, "rb")
    os.close(read_fd)
    return fh, first_byte

def _get_fifo_write_handle(filepath: str, timeout: int=10) -> BinaryIO:
    """
    Check that FIFO at `filepath` is writable and return high level file handle.
    If FIFO is not writable after `timeout`, raise `OSError`.
    """
    for _ in range(timeout):
        try:
            os.close(os.open(filepath, os.O_WRONLY | os.O_NONBLOCK))
            break
        except OSError:
            time.sleep(1)
    else:
        raise OSError("FIFO pipe never opened for reading")
    return open(filepath, "wb")
