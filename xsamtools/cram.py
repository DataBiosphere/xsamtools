"""
Tools for working with CRAM files.

CRAM/CRAI spec here:
http://samtools.github.io/hts-specs/CRAMv3.pdf
"""
import os
import datetime
import logging
import gzip
import io

from collections import namedtuple
from tempfile import TemporaryDirectory
from typing import Optional, Dict, Any, Union
from urllib.request import urlretrieve
from terra_notebook_utils import xprofile

from xsamtools import gs_utils
from xsamtools.utils import run

CramLocation = namedtuple("CramLocation", "chr alignment_start alignment_span offset slice_offset slice_size")
log = logging.getLogger(__name__)

def read_fixed_length_cram_file_definition(fh: io.BytesIO) -> Dict[str, Union[int, str]]:
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
        'major_version': decode_int8(fh),
        'minor_version': decode_int8(fh),
        'file_id': fh.read(20).decode('utf-8')
    }

def read_cram_container_header(fh: io.BytesIO) -> Dict[str, Any]:
    """
    From an open BytesIO handle, returns a dictionary of the contents of a CRAM container header.
    The file definition is followed by one or more containers with the following header structure where the container
    content is stored in the ‘blocks’ field:
    -----------------------------------------------------------------------------------------------------------
    | Data Type      Name                        Value                                                        |
    -----------------------------------------------------------------------------------------------------------
    | INT32          length                      The sum of the lengths of all blocks in this container       |
    |                                              (headers and data); equal to the total byte length of the  |
    |                                              container minus the byte length of this header structure.  |
    | ITF-8           reference sequence id      Reference sequence identifier or:                            |
    |                                               -1 for unmapped reads                                     |
    |                                               -2 for multiple reference sequences                       |
    |                                            All slices in this container must have a reference sequence  |
    |                                              id matching this value.                                    |
    | ITF-8           reference start position   The alignment start position or 0 if the container is        |
    |                                              multiple-reference or contains unmapped unplaced reads     |
    | ITF-8           alignment span             The length of the alignment or 0 if the container is         |
    |                                              multiple-reference or contains unmapped unplaced reads.    |
    | ITF-8           number of records          Number of records in the container.                          |
    | LTF-8           record counter             1-based sequential index of records in the file/stream.      |
    | LTF-8           bases                      Number of read bases.                                        |
    | ITF-8           number of blocks           The total number of blocks in this container.                |
    | Array<ITF-8>    landmarks                  The locations of slices in this container as byte offsets    |
    |                                             from the end of this container header, used for random      |
    |                                             access indexing. The landmark count must equal the slice    |
    |                                             count.  Since the block before the first slice is the       |
    |                                             compression header, landmarks[0] is equal to the byte       |
    |                                             length of the compression header.                           |
    | INT             crc32                      CRC32 hash of the all the preceding bytes in the container.  |
    -----------------------------------------------------------------------------------------------------------
    """
    return {
        "length": decode_int32(fh),
        "reference_sequence_id": decode_itf8(fh),
        "starting_position": decode_itf8(fh),
        "alignment_span": decode_itf8(fh),
        "number_of_records": decode_itf8(fh),
        "record_counter": decode_ltf8(fh),
        "bases": decode_ltf8(fh),
        "number_of_blocks": decode_itf8(fh),
        "landmark": decode_itf8_array(fh),
        "crc_hash": fh.read(4)
    }

def decode_int32(fh: io.BytesIO) -> int:
    """A CRAM defined 32-bit signed integer type."""
    return int.from_bytes(fh.read(4), byteorder='little', signed=True)

def decode_int8(fh: io.BytesIO) -> int:
    """
    Read a single byte as an unsigned integer.

    This data type isn't given a special name like "ITF-8" or "int32" in the spec, and is only used twice in the
    file descriptor as a special case, and as a convenience to construct other data types, like ITF-8 and LTF-8.
    """
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
    int1 = decode_int8(fh)

    if (int1 & 128) == 0:
        return int1

    elif (int1 & 64) == 0:
        int2 = decode_int8(fh)
        return ((int1 & 127) << 8) | int2

    elif (int1 & 32) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        return ((int1 & 63) << 16) | int2 << 8 | int3

    elif (int1 & 16) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        return ((int1 & 31) << 24) | int2 << 16 | int3 << 8 | int4

    else:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
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

def decode_ltf8(fh: io.BytesIO) -> int:
    """
    Decode integer values with CRAM's LTF-8 protocol (Long Transformation Format - 8 bit).

    - LTF-8 represents a 64-bit Long Unsigned Integer (the long version of the 32-bit ITF-8).
    - LTF-8 allocates 1-9 bytes to store integers, and ITF-8 only allocates 1-5 bytes.

    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/LTF8.java  # noqa
    """
    int1 = decode_int8(fh)

    # same as itf8
    if (int1 & 128) == 0:
        return int1

    # same as itf8
    elif (int1 & 64) == 0:
        int2 = decode_int8(fh)
        return (int1 & 127) << 8 | int2

    # same as itf8
    elif (int1 & 32) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        return (int1 & 63) << 16 | int2 << 8 | int3

    # same as itf8
    elif (int1 & 16) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        return (int1 & 31) << 24 | int2 << 16 | int3 << 8 | int4

    # differs from itf8; doesn't truncate 4 bytes
    elif (int1 & 8) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
        return (int1 & 15) << 32 | (0xFF & int2) << 24 | int3 << 16 | int4 << 8 | int5

    # this is where the number gets too big for itf8
    elif (int1 & 4) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
        int6 = decode_int8(fh)
        return (int1 & 7) << 40 | (0xFF & int2) << 32 | (0xFF & int3) << 24 | (int4 << 16) | (int5 << 8) | int6

    elif (int1 & 2) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
        int6 = decode_int8(fh)
        int7 = decode_int8(fh)
        return (int1 & 3) << 48 | (0xFF & int2) << 40 | (0xFF & int3) << 32 | \
               (0xFF & int4) << 24 | int5 << 16 | int6 << 8 | int7

    # NOTE: int1 is unused here!
    elif (int1 & 1) == 0:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
        int6 = decode_int8(fh)
        int7 = decode_int8(fh)
        int8 = decode_int8(fh)
        return (0xFF & int2) << 48 | (0xFF & int3) << 40 | (0xFF & int4) << 32 | \
               (0xFF & int5) << 24 | int6 << 16 | int7 << 8 | int8

    # NOTE: int1 is also unused here!
    else:
        int2 = decode_int8(fh)
        int3 = decode_int8(fh)
        int4 = decode_int8(fh)
        int5 = decode_int8(fh)
        int6 = decode_int8(fh)
        int7 = decode_int8(fh)
        int8 = decode_int8(fh)
        int9 = decode_int8(fh)
        return (0xFF & int2) << 56 | (0xFF & int3) << 48 | (0xFF & int4) << 40 | (0xFF & int5) << 32 | \
               (0xFF & int6) << 24 | int7 << 16 | int8 << 8 | int9

def encode_ltf8(num: int) -> bytes:
    """
    Encode integer values with CRAM's LTF-8 protocol (Long Transformation Format - 8 bit).
    LTF-8 represents a 64-bit Long Unsigned Integer (the long version of ITF-8).
    The main difference between ITF-8 and LTF-8 is the number of bytes used to encode a single value.
    LTF-8 allocates 1-9 bytes to store integers, and ITF-8 only allocates 1-5 bytes.
    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/LTF8.java  # noqa
    """
    if num >> 7 == 0:
        integers = [num]
    elif num >> 14 == 0:
        integers = [((num >> 8) | 0x80), num & 0xFF]
    elif num >> 21 == 0:
        integers = [((num >> 16) | 0xC0), (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 28 == 0:
        integers = [((num >> 24) | 0xE0), (num >> 16) & 0xFF, (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 35 == 0:
        # differs from itf8; doesn't truncate 4 bytes
        integers = [((num >> 32) | 0xF0), (num >> 24) & 0xFF, (num >> 16) & 0xFF, (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 42 == 0:
        # this is where the number gets too big for itf8
        integers = [((num >> 40) | 0xF8), (num >> 32) & 0xFF, (num >> 24) & 0xFF, (num >> 16) & 0xFF,
                    (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 49 == 0:
        integers = [((num >> 48) | 0xFC), (num >> 40) & 0xFF, (num >> 32) & 0xFF, (num >> 24) & 0xFF,
                    (num >> 16) & 0xFF, (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 56 == 0:
        # note the first byte here is constant
        integers = [0xFE, (num >> 48) & 0xFF, (num >> 40) & 0xFF, (num >> 32) & 0xFF, (num >> 24) & 0xFF,
                    (num >> 16) & 0xFF, (num >> 8) & 0xFF, num & 0xFF]
    elif num >> 64 == 0:
        # note the first byte here is constant
        integers = [0xFF, (num >> 56) & 0xFF, (num >> 48) & 0xFF, (num >> 40) & 0xFF, (num >> 32) & 0xFF,
                    (num >> 24) & 0xFF, (num >> 16) & 0xFF, (num >> 8) & 0xFF, num & 0xFF]
    else:
        raise ValueError(f'Number is too large for an unsigned 64-bit integer: {num}')
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

def write_final_file_with_samtools(cram: str,
                                   crai: Optional[str],
                                   regions: Optional[str],
                                   cram_format: bool,
                                   output: str) -> None:
    region_args = ' '.join(regions.split(',')) if regions else ''
    cram_format_arg = '-C' if cram_format else ''
    if crai:
        crai_arg = f'-X {crai}'
    else:
        log.warning('No crai file present, this may take a while.')
        crai_arg = ''

    # we can get away with a simple split on spaces here because there's nothing complicated going on
    cmd = f'samtools view {cram_format_arg} {cram} {crai_arg} {region_args}'.split()

    log.info(f'Now running: {cmd}')
    run(cmd, stdout=open(output, 'w'), check=True)
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
        staged_cram = os.path.join(staging_dir, 'tmp.cram')
        stage(uri=cram, output=staged_cram)
        if crai:
            staged_crai = os.path.join(staging_dir, 'tmp.crai')
            stage(uri=crai, output=staged_crai)
        else:
            staged_crai = None

        write_final_file_with_samtools(staged_cram, staged_crai, regions, cram_format, output)

    return output
