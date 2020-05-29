import io
import os
from uuid import uuid4
from multiprocessing import cpu_count
from tempfile import NamedTemporaryFile
import subprocess

from terra_notebook_utils import xprofile

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


@xprofile.profile("combine")
def combine(src_bucket_name, src_keys, dst_bucket_name, dst_key):
    readers = [pipes.BlobReaderProcess(src_bucket_name, key) for key in src_keys]
    writer = pipes.BlobWriterProcess(dst_bucket_name, dst_key)
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

def _get_reader(path):
    if path.startswith("gs://"):
        bucket, key = path[5:].split("/", 1)
        return pipes.BlobReaderProcess(bucket, key)
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
