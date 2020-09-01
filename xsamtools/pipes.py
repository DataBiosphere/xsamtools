import os
import time
import json
import signal
import logging
import multiprocessing
from uuid import uuid4
from contextlib import AbstractContextManager
from concurrent.futures import ProcessPoolExecutor, Future, as_completed
from typing import Any, Tuple, Dict, IO, BinaryIO, Optional

import gs_chunked_io as gscio
from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT

from xsamtools import gs_utils


logger = logging.getLogger(__name__)

def log_info(**kwargs):
    logger.info(json.dumps(kwargs, indent=2))

class FIFOBrokenPipeException(Exception):
    pass

def _Timeout(timeout_attribute: str):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            def _on_sigalarm(signum, frame):
                raise FIFOBrokenPipeException(f"Failed to open main FIFO pipe for mode '{self.mode}'")

            timeout_seconds = getattr(self, timeout_attribute)
            signal.signal(signal.SIGALRM, _on_sigalarm)
            signal.alarm(timeout_seconds)
            ret_val = func(self, *args, **kwargs)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.alarm(0)
            return ret_val
        return wrapper
    return decorator

class FIFOPipeProcess(AbstractContextManager):
    """
    Open a named pipe on the filesystem and launch a subprocess that either reads or writes to the pipe.
    For more information on named pipes, see:
    https://man7.org/linux/man-pages/man7/fifo.7.html
    https://docs.python.org/3.8/library/os.html#os.mkfifo

    Pipe modes:
    "wb->rb" Write on the subprocess side, read on the main process.
    "rb<-wb" Read on the subprocess side, write on the main process.
    """
    _pipe_futures: Dict[Any, Future] = dict()
    _manager: Optional[Any] = None

    def __init__(self,
                 executor: ProcessPoolExecutor,
                 *args,
                 filepath: Optional[str]=None,
                 subprocess_handle_timeout_seconds: int=60,
                 handle_timeout_seconds: int=40,
                 **kwargs):
        self.mode = kwargs.get("mode", "wb->rb")
        assert self.mode in ["wb->rb", "rb<-wb"]
        self.filepath = filepath or f"/tmp/{uuid4()}"
        os.mkfifo(self.filepath)
        self._closed = False

        self._subprocess_handle_timeout_seconds = subprocess_handle_timeout_seconds
        self._handle_timeout_seconds = handle_timeout_seconds
        self._handle: Optional[IO] = None

        self._status = FIFOPipeProcess.get_manager().dict()
        self._status['ready'] = False

        # assigning future to an instance property causes a deadlock. Why?
        FIFOPipeProcess._pipe_futures[self] = executor.submit(self._initiate_subprocess_and_run)
        self.wait_for_ready()

    @_Timeout("_handle_timeout_seconds")
    def wait_for_ready(self):
        while not self._status['ready']:
            time.sleep(1)

    @property
    def future(self):
        return type(self)._pipe_futures[self]

    @classmethod
    def get_manager(cls):
        cls._manager = cls._manager or multiprocessing.Manager()
        return cls._manager

    @_Timeout("_handle_timeout_seconds")
    def get_handle(self) -> IO:
        """
        Get the FIFO file handle on the main process. This should only be called from the main process.
        """
        self._handle = open(self.filepath, self.mode[-2:])
        return self._handle

    @_Timeout("_subprocess_handle_timeout_seconds")
    def _get_subprocess_handle(self) -> IO:
        self._status['ready'] = True
        return open(self.filepath, self.mode[:2])

    def _initiate_subprocess_and_run(self):
        with self._get_subprocess_handle() as handle:
            log_info(action="Opened handle on FIFO pipe", mode=self.mode, filepath=f"{self.filepath}")
            self.run(handle)

    def run(self, handle: IO):
        raise NotImplementedError()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._handle is not None:
                self._handle.close()
            for f in as_completed([self.future], timeout=300):
                pass
            os.unlink(self.filepath)
            self.future.result()
            log_info(action="Closing FIFO pipe", mode=self.mode, filepath=f"{self.filepath}")

    def __enter__(self):
        return self.get_handle()

    def __exit__(self, *args, **kwargs):
        self.close()

class BlobReaderProcess(FIFOPipeProcess):
    def __init__(self, url: str, executor: ProcessPoolExecutor):
        self.url = url
        self._shared_dict = FIFOPipeProcess.get_manager().dict()
        super().__init__(executor, mode="wb->rb")
        log_info(action="Opening blob reader FIFO", mode=self.mode, url=url, filepath=f"{self.filepath}")

    def run(self, fh: IO):
        blob = gs_utils._blob_for_url(self.url)
        with gscio.Reader(blob, threads=1) as blob_reader:
            while True:
                data = bytearray(blob_reader.read(blob_reader.chunk_size))
                if not data:
                    break
                while data:
                    if self._shared_dict.get('stop'):
                        return
                    try:
                        k = fh.write(data)
                        data = data[k:]
                    except BrokenPipeError:
                        time.sleep(1)

    def close(self):
        if not self._closed:
            self._shared_dict['stop'] = True
            super().close()

class BlobWriterProcess(FIFOPipeProcess):
    def __init__(self, bucket_name: str, key: str, executor: ProcessPoolExecutor):
        self.bucket_name = bucket_name
        self.key = key
        super().__init__(executor, mode="rb<-wb")
        log_info(action="Opening blob writer FIFO",
                 mode=self.mode,
                 bucket=bucket_name,
                 key=key,
                 filepath=f"{self.filepath}")

    def run(self, fh: IO):
        bucket = gs.get_client().bucket(self.bucket_name)
        with gscio.Writer(self.key, bucket, threads=1) as blob_writer:
            while True:
                data = fh.read(blob_writer.chunk_size)
                if not data:
                    break
                blob_writer.write(data)

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
