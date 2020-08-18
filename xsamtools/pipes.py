import os
import time
import json
import logging
import multiprocessing
from uuid import uuid4
from contextlib import AbstractContextManager
from concurrent.futures import ProcessPoolExecutor, Future, as_completed
from typing import Tuple, BinaryIO, Optional

import gs_chunked_io as gscio
from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT

from xsamtools import gs_utils


logger = logging.getLogger(__name__)

def log_info(**kwargs):
    logger.info(json.dumps(kwargs, indent=2))

class BlobReaderProcess(AbstractContextManager):
    def __init__(self, url: str, executor: ProcessPoolExecutor, filepath: str=None):
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.queue = multiprocessing.Manager().Queue()
        self.future = executor.submit(BlobReaderProcess.run, url, self.filepath, self.queue)
        self.future.add_done_callback(check_future_result)
        self._closed = False

    @staticmethod
    def run(url: str, filepath: str, queue: multiprocessing.Queue):
        blob = gs_utils._blob_for_url(url)
        with open(filepath, "wb") as fh:
            with gscio.Reader(blob, threads=1) as blob_reader:
                log_info(action="Starting read pipe", url=f"{url}", key=f"{blob.name}", filepath=f"{filepath}")
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
        log_info(action="Finished read pipe", url=f"{url}", key=f"{blob.name}", filepath=f"{filepath}")

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
            os.unlink(self.filepath)
        log_info(action="Closing read pipe", filepath=f"{self.filepath}")

    def __exit__(self, *args, **kwargs):
        self.close()

class BlobWriterProcess(AbstractContextManager):
    def __init__(self, bucket_name: str, key: str, executor: ProcessPoolExecutor, filepath: Optional[str]=None):
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self.future = executor.submit(BlobWriterProcess.run, bucket_name, key, self.filepath)
        self.future.add_done_callback(check_future_result)
        self._closed = False

    @staticmethod
    def run(bucket_name: str, key: str, filepath: str):
        bucket = gs.get_client().bucket(bucket_name)
        with open(filepath, "rb") as fh:
            with gscio.Writer(key, bucket, threads=1) as blob_writer:
                log_info(action="Starting write pipe", bucket=f"{bucket_name}", key=f"{key}", filepath=f"{filepath}")
                while True:
                    data = fh.read(blob_writer.chunk_size)
                    if not data:
                        break
                    blob_writer.write(data)
        log_info(action="Finished write pipe", bucket=f"{bucket_name}", key=f"{key}", filepath=f"{filepath}")

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
            os.unlink(self.filepath)
        log_info(action="Closing write pipe", filepath=f"{self.filepath}")

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
