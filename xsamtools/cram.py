"""
Tools for working with CRAM files.

CRAM/CRAI spec here:
http://samtools.github.io/hts-specs/CRAMv3.pdf
"""
import os
import gzip
import codecs
import subprocess
import datetime
import copy
import logging
import numpy as np

from uuid import uuid4
from typing import List, Tuple, Optional
from collections import namedtuple
from urllib.request import urlretrieve
from google.cloud.storage import Blob
from terra_notebook_utils import xprofile, gs

CramLocation = namedtuple("CramLocation", "chr alignment_start alignment_span offset slice_offset slice_size")
log = logging.getLogger(__name__)


def download_full_gs(gs_path: str, output_filename: str = None):
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else os.path.abspath(os.path.basename(key_name))
    bucket = gs.get_client().get_bucket(bucket_name)
    blob = Blob(key_name, bucket)
    blob.download_to_filename(output_filename)
    log.debug(f'Entire file "{gs_path}" downloaded to: {output_filename}')
    return output_filename


def download_sliced_gs(gs_path: str, ordered_slices: List[Tuple[int, int]], output_filename: str = None):
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else key_name
    bucket = gs.get_client().get_bucket(bucket_name)
    blob = Blob(key_name, bucket)
    with open(output_filename, "wb") as f:
        for start, end in ordered_slices:
            # TODO: google raises google.resumable_media.common.DataCorruption when checksumming (erroneously?)
            new_string = blob.download_as_bytes(start=start, end=end, raw_download=False, checksum=None)
            f.seek(start)
            f.write(new_string)
    log.debug(f'Sliced file "{gs_path}" downloaded to: {output_filename}')
    return output_filename


def decode_itf8_array(handle, size=None):
    number_of_items_in_array = decode_itf8(handle) if size is None else size
    itf8_array = []
    for _ in range(number_of_items_in_array):
        itf8_array.append(decode_itf8(handle))
    return itf8_array


def next_int(fh):
    return int.from_bytes(fh.read(1), byteorder='little', signed=False)


def encode_itf8(value):
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

    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/ITF8.java#L12
    """
    if value < 2 ** 7:
        integers = [value]
    elif value < 2 ** 14:
        integers = [((value >> 8) | 0x80), (value & 0xFF)]
    elif value < 2 ** 21:
        integers = [((value >> 16) | 0xC0), ((value >> 8) & 0xFF), (value & 0xFF)]
    elif value < 2 ** 28:
        integers = [((value >> 24) | 0xE0), ((value >> 16) & 0xFF), ((value >> 8) & 0xFF), (value & 0xFF)]
    elif value < 2 ** 32:
        integers = [((value >> 28) | 0xF0), ((value >> 20) & 0xFF), ((value >> 12) & 0xFF), ((value >> 4) & 0xFF), (value & 0xFF)]
    else:
        raise ValueError('Number is too large for an unsigned 32-bit integer.')
    return bytes(integers)


def decode_itf8(fh):
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

    Source: https://github.com/samtools/htsjdk/blob/b24c9521958514c43a121651d1fdb2cdeb77cc0b/src/main/java/htsjdk/samtools/cram/io/ITF8.java#L12
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


def file_definition(fh):
    return {
        'cram': fh.read(4).decode('utf-8'),
        'major_version': int.from_bytes(fh.read(1), byteorder='big'),
        'minor_version': int.from_bytes(fh.read(1), byteorder='big'),
        'file_id': fh.read(20).decode('utf-8')
    }


def container_header(fh):
    return {
        "length": np.frombuffer(fh.read(np.dtype(np.int32).itemsize), np.dtype(np.int32))[0],
        "reference_sequence_id": decode_itf8(fh),
        "starting_position": decode_itf8(fh),
        "alignment_span": decode_itf8(fh),
        "number_of_records": decode_itf8(fh),
        "record_counter": decode_itf8(fh),
        "bases": decode_itf8(fh),
        "number_of_blocks": decode_itf8(fh),
        "landmark": decode_itf8_array(fh),
        "crc_hash": fh.read(4)
    }


def read_seq_names_from_sam_header(fh, end_of_header, gzipped=False):
    """Offset must be reset after running this."""
    # TODO: Terminate reading without explicit "end_of_header"
    sequence = {}
    i = 0
    handle = fh if not gzipped else gzip.GzipFile(fileobj=fh)
    try:
        for line in handle:
            if b'@SQ' in line:
                for tag in line.split(b'\t'):
                    if tag == b'@SQ':
                        i += 1
                        sequence[i] = []
                    elif tag[:2] in [b'SN', b'AN']:
                        tag_value = tag[3:]
                        assert tag_value not in sequence
                        sequence[tag_value] = i
            if fh.tell() >= end_of_header:
                return sequence
    except gzip.BadGzipFile:
        return sequence


def find_next_gzip_marker(fh):
    c1 = fh.read(1)
    while True:
        c2 = fh.read(1)
        if c1 == b'\x1f' and c2 == b'\x8b':
            fh.seek(-2, 1)
            return fh.tell()
        else:
            c1 = copy.copy(c2)


def sam_header(fh, end_of_header):
    # TODO: Make this cleaner and return unicode!
    where_was_i = fh.tell()

    # attempt to parse raw header
    seq_map = read_seq_names_from_sam_header(fh, end_of_header)
    if seq_map:
        return seq_map

    # header is compressed, reset and try again
    fh.seek(where_was_i)
    find_next_gzip_marker(fh)
    seq_map = read_seq_names_from_sam_header(fh, end_of_header, gzipped=True)
    assert seq_map, f'Something went wrong reading the cram header (no seq name mappings determined).'
    return seq_map


def read_gs_cram_file_header(output_cram, end_of_header):
    with open(output_cram, "rb") as fh:
        file_definition(fh)
        container_header(fh)
        seq_map = sam_header(fh, end_of_header)
    return seq_map


def write_final_file_with_samtools(cram: str, crai: str, regions: str, cram_format: bool, output: str):
    region_args = ' '.join(regions.split(',')) if regions else ''
    cram_format = '-C' if cram_format else ''
    cmd = f'samtools view {cram_format} {cram} -X {crai} {region_args} > {output}'
    log.info(f'Now running: {cmd}')
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    if p.stdout or p.stderr:
        log.debug(f'\nstdout: {p.stdout}\n')
        log.debug(f'\nstderr: {p.stderr}\n')
    else:
        log.debug(f'Output CRAM successfully generated at: {output}')


def get_crai_indices(crai):
    crai_indices = []
    with open(crai, "rb") as fh:
        with gzip.GzipFile(fileobj=fh) as gzip_reader:
            with codecs.getreader("ascii")(gzip_reader) as reader:
                for line in reader:
                    crai_indices.append(CramLocation(*[int(d) for d in line.split("\t")]))
    return crai_indices


def get_block_slice_map(crai_indices, seq_names):
    slices = []
    slice_start = 0
    truncated_section = False
    for i, crai_line in enumerate(crai_indices):
        slice_end = slice_start + 200 if truncated_section else crai_line.offset
        if not slices or (slices[-1][1] != slice_start):
            slices.append((slice_start, slice_end))
        else:
            slices[-1] = (slices[-1][0], slice_end)
        slice_start = crai_line.offset
        truncated_section = False if crai_line.chr in seq_names else True

    if not slices or (slices[-1][1] != slice_start):
        slices.append((slice_start, None))
    else:
        slices[-1] = (slices[-1][0], None)

    return slices


def download_sliced_cram(crai: str, cram: str, output_filename: str, regions: str):
    """Assumes that a crai is local and that the cram is a google bucket path."""
    assert os.path.exists(crai), crai
    assert cram.startswith('gs://'), cram
    crai_indices = get_crai_indices(crai)
    end_of_first_block = crai_indices[1].offset
    # TODO: Read as a streamed string

    # download the cram header contents
    tmp_header_file = f'{output_filename}.tmp-header.cram'
    local_cram = download_sliced_gs(
        gs_path=cram,
        ordered_slices=[(0, end_of_first_block)],
        output_filename=tmp_header_file)
    # decipher the cram header contents in order to map block numbers to common names like "chr1"
    seq_map = read_gs_cram_file_header(local_cram, end_of_first_block)

    seq_names = []
    for region in regions.split(','):
        seq_name = region.split(':')[0]
        if seq_name.encode('utf-8') in seq_map:
            seq_names.append(seq_map[seq_name.encode('utf-8')])

    if seq_names:
        slices = get_block_slice_map(crai_indices, seq_names)
        download_sliced_gs(gs_path=cram, ordered_slices=slices, output_filename=output_filename)
    else:
        # this occurs when a user specifies chromosomes that do not exist
        # samtools does not error on this and produces an empty output
        # TODO: handle this better!
        download_full_gs(gs_path=cram, output_filename=output_filename)


def stage_crai(crai: str, output: str):
    if crai.startswith('gs://'):
        download_full_gs(crai, output_filename=output)
    elif crai.startswith('file://'):
        os.link(crai[len('file://'):], output)
    elif crai.startswith('http://') or crai.startswith('https://'):
        urlretrieve(crai, output)
    elif '://' not in crai:
        os.link(crai, output)
    else:
        raise NotImplementedError(f'Unsupported format: {crai}')


def stage_cram(cram: str, crai: str, regions: str, output: str, slice_cloud_files: bool):
    if cram.startswith('gs://'):
        if regions and slice_cloud_files:
            # attempt to only download the relevant portions/slices of the file
            download_sliced_cram(cram=cram, crai=crai, regions=regions, output_filename=output)
        else:
            # if there is no subset of regions specified, we need to download the entire cram file
            download_full_gs(cram, output_filename=output)
    elif cram.startswith('file://'):
        os.link(cram[len('file://'):], output)
    elif cram.startswith('http://') or cram.startswith('https://'):
        # TODO: not sure if we can slice these or if there is any desire to?
        urlretrieve(cram, output)
    elif '://' not in cram:
        os.link(cram, output)
    else:
        raise NotImplementedError(f'Unsupported format: {cram}')


def stage_gs_files_locally(cram: str, crai: str, regions: Optional[str], slice_cloud_files: bool):
    # samtools exhibits odd behavior sometimes if the cram and crai are in separate folders; keep them together
    staging_dir = f'/tmp/{uuid4()}/'
    os.makedirs(staging_dir)
    staged_crai = os.path.join(staging_dir, f'tmp.crai')
    staged_cram = os.path.join(staging_dir, f'tmp.cram')

    stage_crai(crai=crai, output=staged_crai)
    stage_cram(cram=cram, crai=staged_crai, regions=regions, output=staged_cram, slice_cloud_files=slice_cloud_files)

    return staged_cram, staged_crai


def timestamped_filename(cram_format):
    time_stamp = str(datetime.datetime.now()).split('.')[0].replace(':', '').replace(' ', '-')
    extension = 'cram' if cram_format else 'sam'
    # NOTE: schema output (gs:// or file://) is preserved
    return os.path.abspath(f'{time_stamp}.output.{extension}')


@xprofile.profile("xsamtools cram view")
def view(cram: str, crai: str, regions: Optional[str], output: Optional[str] = None, cram_format: bool = True,
         slice_cloud_files: bool = True):
    output = output or timestamped_filename(cram_format)
    staged_files = []

    try:
        if cram.startswith('gs://') or crai.startswith('gs://'):
            cram, crai = staged_files = stage_gs_files_locally(cram=cram, crai=crai, regions=regions,
                                                               slice_cloud_files=slice_cloud_files)
        write_final_file_with_samtools(cram=cram, crai=crai, regions=regions, cram_format=cram_format, output=output)
    finally:
        # clean up
        for staged_file in staged_files:
            if os.path.exists(staged_file):
                os.remove(staged_file)

    return output
