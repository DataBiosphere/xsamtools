import io
import os
from uuid import uuid4
from multiprocessing import cpu_count
from tempfile import NamedTemporaryFile
import subprocess

from terra_notebook_utils import xprofile, drs

from xsamtools import pipes, vcf, samtools


cores_available = cpu_count()


def _merge(input_filepaths, output_filepath):
    subprocess.run([samtools.paths['bcftools'],
                    "merge",
                    "--no-index",
                    "-o", output_filepath,
                    "-O", "z",
                    "--threads", f"{2 * cores_available}"]
                   + [fp for fp in input_filepaths])

def _view(input_filepath, output_filepath, samples):
    with NamedTemporaryFile() as tf:
        with open(tf.name, "w") as fh:
            fh.write(os.linesep.join(samples))
        subprocess.run([samtools.paths['bcftools'],
                        "view",
                        "-o", output_filepath,
                        "-O", "z",
                        "-S", tf.name,
                        "--threads", f"{2 * cores_available}",
                        input_filepath])

def _stats(input_filepath):
    subprocess.run([samtools.paths['bcftools'],
                    "stats",
                    "--threads", f"{2 * cores_available}",
                    input_filepath])

@xprofile.profile("combine")
def combine(src_files, output_file):
    readers = [_get_reader(fp) for fp in src_files]
    writer = _get_writer(output_file)
    try:
        _merge([r.filepath for r in readers], writer.filepath)
    finally:
        for reader in readers:
            reader.close()
        writer.close()

@xprofile.profile("subsample")
def subsample(src_path: str, dst_path: str, samples):
    reader = _get_reader(src_path)
    writer = _get_writer(dst_path)
    try:
        _view(reader.filepath, writer.filepath, samples)
    finally:
        reader.close()
        writer.close()

def stats(src_path):
    reader = _get_reader(src_path)
    try:
        _stats(reader.filepath)
    finally:
        reader.close()

def _get_reader(path):
    if path.startswith("gs://") or path.startswith("drs://"):
        return pipes.BlobReaderProcess(path)
    else:
        fh = open(path)
        fh.filepath = path
        return fh

def _get_writer(path):
    if path.startswith("gs://"):
        bucket, key = path[5:].split("/", 1)
        return pipes.BlobWriterProcess(bucket, key)
    else:
        fh = open(path, "wb")
        fh.filepath = path
        return fh
