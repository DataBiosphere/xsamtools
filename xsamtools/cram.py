"""
Tools for working with CRAM files.

CRAM/CRAI spec here:
http://samtools.github.io/hts-specs/CRAMv3.pdf
"""
import os
import subprocess
import datetime
import logging
import gzip
import io

from collections import namedtuple
from tempfile import TemporaryDirectory
from typing import Optional
from urllib.request import urlretrieve
from terra_notebook_utils import xprofile

from xsamtools import gs_utils

CramLocation = namedtuple("CramLocation", "chr alignment_start alignment_span offset slice_offset slice_size")
log = logging.getLogger(__name__)

def read_fixed_length_cram_file_definition(fh: io.BytesIO):
    """
    This definition is always the first 26 bytes of a cram file.

    From CRAM spec 3.0 (22 Jun 2020):

    -------------------------------------------------------------------------------------------------
    | Data type       Name                   Value                                                  |
    -------------------------------------------------------------------------------------------------
    | byte[4]         format magic number    CRAM (0x43 0x52 0x41 0x4d)                             |
    | unsigned byte   major format number    3 (0x3)                                                |
    | unsigned byte   minor format number    0 (0x0)                                                |
    | byte[20]        file id                CRAM file identifier (e.g. file name or SHA1 checksum) |
    -------------------------------------------------------------------------------------------------

    Valid CRAM major.minor version numbers are as follows:
        1.0 The original public CRAM release.
        2.0 The first CRAM release implemented in both Java and C; tidied up implementation vs specification
        differences in 1.0.
        2.1 Gained end of file markers; compatible with 2.0.
        3.0 Additional compression methods; header and data checksums; improvements for unsorted data.
    """
    return {
        'cram': fh.read(4).decode('utf-8'),
        'major_version': int.from_bytes(fh.read(1), byteorder='little'),  # order shouldn't matter with 1 byte
        'minor_version': int.from_bytes(fh.read(1), byteorder='little'),  # but it's required
        'file_id': fh.read(20).decode('utf-8')
    }

def next_int(fh: io.BytesIO) -> int:
    return int.from_bytes(fh.read(1), byteorder='little', signed=False)

def decode_itf8(fh: io.BytesIO) -> int:
    """
     * Decode int values with CRAM's ITF8 protocol.
     *
     * ITF8 encodes ints as 1 to 5 bytes depending on the highest set bit.
     *
     * (using 1-based counting)
     * If highest bit < 8:
     *      write out [bits 1-8]
     * Highest bit = 8-14:
     *      write a byte 1,0,[bits 9-14]
     *      write out [bits 1-8]
     * Highest bit = 15-21:
     *      write a byte 1,1,0,[bits 17-21]
     *      write out [bits 9-16]
     *      write out [bits 1-8]
     * Highest bit = 22-28:
     *      write a byte 1,1,1,0,[bits 25-28]
     *      write out [bits 17-24]
     *      write out [bits 9-16]
     *      write out [bits 1-8]
     * Highest bit > 28:
     *      write a byte 1,1,1,1,[bits 29-32]
     *      write out [bits 21-28]                      **** note the change in pattern here
     *      write out [bits 13-20]
     *      write out [bits 5-12]
     *      write out [bits 1-8]
    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/ITF8.java#L12  # noqa
    """
    int1 = next_int(fh)

    if (int1 & 128) == 0:
        return int1

    elif (int1 & 64) == 0:
        int2 = next_int(fh)
        return ((int1 & 127) << 8) | int2

    elif (int1 & 32) == 0:
        int2 = next_int(fh)
        int3 = next_int(fh)
        return ((int1 & 63) << 16) | int2 << 8 | int3

    elif (int1 & 16) == 0:
        int2 = next_int(fh)
        int3 = next_int(fh)
        int4 = next_int(fh)
        return ((int1 & 31) << 24) | int2 << 16 | int3 << 8 | int4

    else:
        int2 = next_int(fh)
        int3 = next_int(fh)
        int4 = next_int(fh)
        int5 = next_int(fh)
        return ((int1 & 15) << 28) | int2 << 20 | int3 << 12 | int4 << 4 | (15 & int5)

def encode_itf8(num: int) -> bytes:
    """
     * Encodes int values with CRAM's ITF8 protocol.
     *
     * ITF8 encodes ints as 1 to 5 bytes depending on the highest set bit.
     *
     * (using 1-based counting)
     * If highest bit < 8:
     *      write out [bits 1-8]
     * Highest bit = 8-14:
     *      write a byte 1,0,[bits 9-14]
     *      write out [bits 1-8]
     * Highest bit = 15-21:
     *      write a byte 1,1,0,[bits 17-21]
     *      write out [bits 9-16]
     *      write out [bits 1-8]
     * Highest bit = 22-28:
     *      write a byte 1,1,1,0,[bits 25-28]
     *      write out [bits 17-24]
     *      write out [bits 9-16]
     *      write out [bits 1-8]
     * Highest bit > 28:
     *      write a byte 1,1,1,1,[bits 29-32]
     *      write out [bits 21-28]                      **** note the change in pattern here
     *      write out [bits 13-20]
     *      write out [bits 5-12]
     *      write out [bits 1-8]
    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/ITF8.java#L12  # noqa
    """
    if num < 2 ** 7:
        integers = [num]
    elif num < 2 ** 14:
        integers = [((num >> 8) | 0x80), (num & 0xFF)]
    elif num < 2 ** 21:
        integers = [((num >> 16) | 0xC0), ((num >> 8) & 0xFF), (num & 0xFF)]
    elif num < 2 ** 28:
        integers = [((num >> 24) | 0xE0), ((num >> 16) & 0xFF), ((num >> 8) & 0xFF), (num & 0xFF)]
    elif num < 2 ** 32:
        integers = [((num >> 28) | 0xF0), ((num >> 20) & 0xFF), ((num >> 12) & 0xFF), ((num >> 4) & 0xFF), (num & 0xFF)]
    else:
        raise ValueError('Number is too large for an unsigned 32-bit integer.')
    return bytes(integers)

def decode_itf8_array(handle: io.BytesIO, size: Optional[int] = None):
    """
    Decodes an itf8 array from a BytesIO stream.

    The spec either defines the length of the expected array as the first byte of the BytesIO stream...
    OR it's explicitly in the spec (e.g. Array[4] always has a length of four), so sometimes we need to rely on the
    specification itself to document the array size and sometimes we can only determine the size from the CRAM file.
    """
    if size is None:
        size = decode_itf8(handle)
    return [decode_itf8(handle) for _ in range(size)]

def get_crai_indices(crai):
    crai_indices = []
    with open(crai, "rb") as fh:
        with gzip.GzipFile(fileobj=fh) as gzip_reader:
            with io.TextIOWrapper(gzip_reader, encoding='ascii') as reader:
                for line in reader:
                    crai_indices.append(CramLocation(*[int(d) for d in line.split("\t")]))
    return crai_indices

def download_full_gs(gs_path: str, output_filename: str = None) -> str:
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else os.path.abspath(os.path.basename(key_name))
    blob = gs_utils._blob_for_url(gs_path)
    blob.download_to_filename(output_filename)
    log.debug(f'Entire file "{gs_path}" downloaded to: {output_filename}')
    return output_filename

def format_and_check_cram(cram: str) -> str:
    if cram.startswith('file://'):
        cram = cram[len('file://'):]
    if ':' in cram:
        raise NotImplementedError(f'Unsupported schema: {cram}')
    cram = os.path.abspath(cram)
    assert os.path.exists(cram)
    return cram

def pipe_two_commands(cmd1, cmd2):
    log.info(f'Now running proxy for: "{" ".join(cmd1)} | {" ".join(cmd2)}"')
    p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
    p2 = subprocess.Popen(cmd2, stdin=p1.stdout)
    p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
    p2.communicate()

def write_final_file_with_samtools(cram: str,
                                   crai: Optional[str],
                                   regions: Optional[str],
                                   cram_format: bool,
                                   output: str) -> None:
    if not crai:
        log.warning('No crai file specified, this might take a long time while samtools generates it...')

    cmd1 = ['scripts/stream_cloud_file', '--path', cram] if cram.startswith('gs://') else ['cat', cram]

    cmd2 = ['samtools', 'view', '-']
    if cram_format:
        cmd2 += ['-C']
    if crai:
        cmd2 += ['-X', crai]
    if regions:
        cmd2 += regions.split(',')
    if output:
        cmd2 += ['-o', output]

    # samtools seems to produce a benign(?) error: 'samtools view: error closing "-": -1'
    # this appears to happen after the cram file is written however, & doesn't seem to affect the result
    pipe_two_commands(cmd1, cmd2)
    log.debug(f'Output CRAM successfully generated at: {output}')

def stage(uri: str, output: str) -> None:
    """
    Make a file available locally for samtools to use.

    This also allows the file to be placed in the same folder as associated
    files, like cram and crai, which samtools can be picky about.
    """
    if uri.startswith('gs://'):
        download_full_gs(uri, output_filename=output)
    elif uri.startswith('file://'):
        if os.path.abspath(uri[len('file://'):]) != os.path.abspath(output):
            os.link(uri[len('file://'):], output)
    elif uri.startswith('http://') or uri.startswith('https://'):
        urlretrieve(uri, output)
    elif ':' not in uri:
        if os.path.abspath(uri) != os.path.abspath(output):
            os.link(uri, output)
    else:
        raise NotImplementedError(f'Unsupported format: {uri}')

def timestamped_filename(cram_format: bool) -> str:
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
    extension = 'cram' if cram_format else 'sam'
    return os.path.abspath(f'{time_stamp}.output.{extension}')

@xprofile.profile("xsamtools cram view")
def view(cram: str,
         crai: Optional[str],
         regions: Optional[str],
         output: Optional[str] = None,
         cram_format: bool = True) -> str:
    output = output or timestamped_filename(cram_format)
    output = output[len('file://'):] if output.startswith('file://') else output
    assert ':' not in output, f'Unsupported schema for output: "{output}".\n' \
                              f'Only local file outputs are currently supported.'

    with TemporaryDirectory() as staging_dir:
        # defaults
        staged_cram = cram
        staged_crai = crai

        if not cram.startswith('gs://'):
            # gs files are skipped because they will be streamed from the cloud directly
            # staged files are linked into the stage dir and then streamed with cat
            staged_cram = os.path.join(staging_dir, 'tmp.cram')
            stage(uri=cram, output=staged_cram)

        if crai:
            # always stage a crai if the user provides one, allowing samtools to find it...
            # otherwise samtools may generate one which can take a very long time
            staged_crai = os.path.join(staging_dir, 'tmp.crai')
            stage(uri=crai, output=staged_crai)

        write_final_file_with_samtools(staged_cram, staged_crai, regions, cram_format, output)

    return output
