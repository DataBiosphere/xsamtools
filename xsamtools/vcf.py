import io
from uuid import uuid4
from multiprocessing import cpu_count
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


@xprofile.profile("combine")
def combine(src_bucket_name, src_keys, dst_bucket_name, dst_key):
    readers = [pipes.BlobReaderProcess(src_bucket_name, key) for key in src_keys]
    writer = pipes.BlobWriterProcess(dst_bucket_name, dst_key)
    try:
        _merge([r.filepath for r in readers], writer.filepath)
    except Exception:
        raise
    finally:
        for reader in readers:
            reader.close()
        writer.close()
