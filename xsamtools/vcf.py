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
def subsample(src_bucket_name, src_key, dst_bucket_name, dst_key, samples):
    reader = pipes.BlobReaderProcess(src_bucket_name, src_key)
    writer = pipes.BlobWriterProcess(dst_bucket_name, dst_key)
    try:
        _view(reader.filepath, writer.filepath, samples)
    finally:
        reader.close()
        writer.close()
