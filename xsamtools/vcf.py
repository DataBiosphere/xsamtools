import os
from multiprocessing import cpu_count
from tempfile import NamedTemporaryFile
import subprocess
from concurrent.futures import ProcessPoolExecutor
from typing import Union, Sequence

from terra_notebook_utils import xprofile

from xsamtools import pipes, samtools, gs_utils


cores_available = cpu_count()

def reject_preset_args(args, preset_args):
    if any(arg in args for arg in preset_args):
        raise ValueError(f'The following args cannot be supplied manually: {preset_args}')

def _merge(input_filepaths: Sequence[str], output_filepath: str, *args: str):
    preset_args = ['-o', '-O', '--threads', '--no-index']
    reject_preset_args(args, preset_args)
    subprocess.run([samtools.paths['bcftools'],
                    "merge",
                    "--no-index",
                    "-o", output_filepath,
                    "-O", "z",
                    "--threads", f"{2 * cores_available}",
                    *args]
                   + [fp for fp in input_filepaths],
                   check=True)

def _view(input_filepath: str, output_filepath: str, samples: Sequence[str], *args: str):
    preset_args = ['-o', '-O', '-S', '--threads']
    reject_preset_args(args, preset_args)
    with NamedTemporaryFile() as tf:
        with open(tf.name, "w") as fh:
            fh.write(os.linesep.join(samples))
        subprocess.run([samtools.paths['bcftools'],
                        "view",
                        "-o", output_filepath,
                        "-O", "z",
                        "-S", tf.name,
                        "--threads", f"{2 * cores_available}",
                        *args,
                        input_filepath],
                       check=True)

def _stats(input_filepath: str, *args: str):
    preset_args = ['--threads']
    reject_preset_args(args, preset_args)
    subprocess.run([samtools.paths['bcftools'],
                    "stats",
                    "--threads", f"{2 * cores_available}",
                    *args,
                    input_filepath],
                   check=True)

@xprofile.profile("combine")
def combine(src_files: Sequence[str], output_file: str, *args: str):
    assert samtools.paths['bcftools']
    gs_utils._assert_access(src_files, [output_file])
    with ProcessPoolExecutor(max_workers=len(src_files) + 1) as e:
        readers = [_get_reader(fp, e) for fp in src_files]
        writer = _get_writer(output_file, e)
        try:
            _merge([r.filepath for r in readers], writer.filepath, *args)
        finally:
            for reader in readers:
                reader.close()
            writer.close()

@xprofile.profile("subsample")
def subsample(src_path: str, dst_path: str, samples, *args: str):
    assert samtools.paths['bcftools']
    gs_utils._assert_access([src_path], [dst_path])
    with ProcessPoolExecutor(max_workers=2) as e:
        reader = _get_reader(src_path, e)
        writer = _get_writer(dst_path, e)
        try:
            _view(reader.filepath, writer.filepath, samples, *args)
        finally:
            reader.close()
            writer.close()

def stats(src_path: str, *args: str):
    assert samtools.paths['bcftools']
    gs_utils._assert_access([src_path], [])
    with ProcessPoolExecutor() as e:
        reader = _get_reader(src_path, e)
        try:
            _stats(reader.filepath, *args)
        finally:
            reader.close()

class _IOStubb:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def close(self):
        pass

def _get_reader(path: str, executor: ProcessPoolExecutor) -> Union[_IOStubb, pipes.BlobReaderProcess]:
    if path.startswith("gs://") or path.startswith("drs://"):
        return pipes.BlobReaderProcess(path, executor)
    else:
        return _IOStubb(path)

def _get_writer(path: str, executor: ProcessPoolExecutor) -> Union[_IOStubb, pipes.BlobWriterProcess]:
    if path.startswith("gs://"):
        bucket, key = path[5:].split("/", 1)
        return pipes.BlobWriterProcess(bucket, key, executor)
    else:
        return _IOStubb(path)
