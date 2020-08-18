import io
import os
from uuid import uuid4
from multiprocessing import cpu_count
from tempfile import NamedTemporaryFile
import subprocess
from concurrent.futures import ProcessPoolExecutor
from typing import Union, Sequence

from terra_notebook_utils import xprofile, drs

from xsamtools import pipes, vcf, samtools, gs_utils


cores_available = cpu_count()


def _merge(input_filepaths: Sequence[str], output_filepath: str):
    subprocess.run([samtools.paths['bcftools'],
                    "merge",
                    "--no-index",
                    "-o", output_filepath,
                    "-O", "z",
                    "--threads", f"{2 * cores_available}"]
                   + [fp for fp in input_filepaths],
                   check=True)

def _view(input_filepath: str, output_filepath: str, samples: Sequence[str]):
    with NamedTemporaryFile() as tf:
        with open(tf.name, "w") as fh:
            fh.write(os.linesep.join(samples))
        subprocess.run([samtools.paths['bcftools'],
                        "view",
                        "-o", output_filepath,
                        "-O", "z",
                        "-S", tf.name,
                        "--threads", f"{2 * cores_available}",
                        input_filepath],
                       check=True)

def _stats(input_filepath: str):
    subprocess.run([samtools.paths['bcftools'],
                    "stats",
                    "--threads", f"{2 * cores_available}",
                    input_filepath],
                   check=True)

@xprofile.profile("combine")
def combine(src_files: Sequence[str], output_file: str):
    assert samtools.paths['bcftools']
    with ProcessPoolExecutor(max_workers=len(src_files) + 1) as e:
        readers = [_get_reader(fp, e) for fp in src_files]
        writer = _get_writer(output_file, e)
        try:
            _merge([r.filepath for r in readers], writer.filepath)
        finally:
            for reader in readers:
                reader.close()
            writer.close()

@xprofile.profile("subsample")
def subsample(src_path: str, dst_path: str, samples):
    assert samtools.paths['bcftools']
    with ProcessPoolExecutor(max_workers=2) as e:
        reader = _get_reader(src_path, e)
        writer = _get_writer(dst_path, e)
        try:
            _view(reader.filepath, writer.filepath, samples)
        finally:
            reader.close()
            writer.close()

def stats(src_path: str):
    assert samtools.paths['bcftools']
    with ProcessPoolExecutor() as e:
        reader = _get_reader(src_path, e)
        try:
            _stats(reader.filepath)
        finally:
            reader.close()

class _IOStubb:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def close(self):
        pass

def _get_reader(path: str, executor: ProcessPoolExecutor) -> Union[_IOStubb, pipes.BlobReaderProcess]:
    if path.startswith("gs://") or path.startswith("drs://"):
        gs_utils._blob_for_url(path, verify_read_access=True)
        return pipes.BlobReaderProcess(path, executor)
    else:
        return _IOStubb(path)

def _get_writer(path: str, executor: ProcessPoolExecutor) -> Union[_IOStubb, pipes.BlobWriterProcess]:
    if path.startswith("gs://"):
        bucket, key = path[5:].split("/", 1)
        assert gs_utils._write_access(bucket)
        return pipes.BlobWriterProcess(bucket, key, executor)
    else:
        return _IOStubb(path)
