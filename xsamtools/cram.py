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
import numpy as np

from uuid import uuid4
from typing import List, Tuple, Optional
from collections import namedtuple
from bitarray import bitarray
from urllib.request import urlretrieve
from google.cloud.storage import Blob
from google.cloud.storage.client import Client
from terra_notebook_utils import xprofile, drs

client = Client(project="platform-dev")
tmp_staging_dir = f'/tmp/{uuid4()}/'
os.makedirs(tmp_staging_dir)
tmp_cram = os.path.join(tmp_staging_dir, f'tmp.cram')
tmp_crai = os.path.join(tmp_staging_dir, f'tmp.crai')


def download_gs(gs_path: str, output_filename: str = None):
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else key_name
    bucket = client.get_bucket(bucket_name)
    blob = Blob(key_name, bucket)
    blob.download_to_filename(output_filename)
    print(f'{gs_path} downloaded to: {output_filename}')
    return output_filename


def download_sliced_gs(gs_path: str, ordered_slices: List[Tuple[int, int]], output_filename: str = None):
    # TODO: use gs_chunked_io instead
    print(gs_path[len('gs://'):].split('/', 1))
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else key_name
    bucket = client.get_bucket(bucket_name)
    blob = Blob(key_name, bucket)
    with open(output_filename, "wb") as f:
        for start, end in ordered_slices:
            new_string = blob.download_as_string(start=start, end=end)
            f.seek(start)
            f.write(new_string)
    print(f'Sliced {gs_path} downloaded to: {output_filename}')
    return output_filename


def bitarray_from_byte(data):
    ba = bitarray(endian='big')
    ba.frombytes(data)
    return ba.to01()


def convert_to_int(input_bits='01010101'):
    # TODO: This needs to be a signed int
    total = 0
    for i, b in enumerate(input_bits[::-1]):
        total += int(b) * (2 ** i)
    return total


def decode_itf8_array(handle, size=None):
    number_of_items_in_array = decode_itf8(handle) if size is None else size
    itf8_array = []
    for _ in range(number_of_items_in_array):
        itf8_array.append(decode_itf8(handle))
    return itf8_array


def decode_itf8(handle):
    """
     * Methods to read and write int values as per ITF8 specification in CRAM.
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

    :param handle:
    :return:
    """
    first_byte = handle.read(1)
    initial_data_block = bitarray_from_byte(first_byte)

    if initial_data_block.startswith('0'):
        final_data_block = initial_data_block

    elif initial_data_block.startswith('10'):
        bits_9_to_14 = initial_data_block.lstrip('10')
        bits_1_to_8 = bitarray_from_byte(handle.read(1))
        final_data_block = bits_1_to_8 + bits_9_to_14

    elif initial_data_block.startswith('110'):
        bits_17_to_21 = initial_data_block.lstrip('110')
        bits_9_to_16 = bitarray_from_byte(handle.read(1))
        bits_1_to_8 = bitarray_from_byte(handle.read(1))
        final_data_block = bits_1_to_8 + bits_9_to_16 + bits_17_to_21

    elif initial_data_block.startswith('1110'):
        bits_25_to_28 = initial_data_block.lstrip('1110')
        bits_17_to_24 = bitarray_from_byte(handle.read(1))
        bits_9_to_16 = bitarray_from_byte(handle.read(1))
        bits_1_to_8 = bitarray_from_byte(handle.read(1))
        final_data_block = bits_1_to_8 + bits_9_to_16 + bits_17_to_24 + bits_25_to_28

    elif initial_data_block.startswith('1111'):
        bits_29_to_32 = initial_data_block.lstrip('1111')
        bits_21_to_28 = bitarray_from_byte(handle.read(1))
        bits_13_to_20 = bitarray_from_byte(handle.read(1))
        bits_5_to_12 = bitarray_from_byte(handle.read(1))
        bits_1_to_8 = bitarray_from_byte(handle.read(1))

        # cut off the overlap
        bits_1_to_4 = bits_1_to_8[:4]
        final_data_block = bits_1_to_4 + bits_5_to_12 + bits_13_to_20 + bits_21_to_28 + bits_29_to_32

    else:
        print('This should never happen')
        exit()

    return convert_to_int(final_data_block)


def file_definition(f):
    CRAM = f.read(4).decode('utf-8')
    major_version = int.from_bytes(f.read(1), byteorder='big')
    minor_version = int.from_bytes(f.read(1), byteorder='big')
    file_id = f.read(20).decode('utf-8')
    return f'{CRAM} {major_version}.{minor_version} {file_id}'


def container_header(fh, show=True):
    length = np.frombuffer(fh.read(np.dtype(np.int32).itemsize), np.dtype(np.int32))[0]
    reference_sequence_id = decode_itf8(fh)
    starting_position = decode_itf8(fh)
    alignment_span = decode_itf8(fh)
    number_of_records = decode_itf8(fh)
    record_counter = decode_itf8(fh)
    bases = decode_itf8(fh)
    number_of_blocks = decode_itf8(fh)
    landmarks = decode_itf8_array(fh)
    if show:
        print("length", length)
        print("reference_sequence_id", reference_sequence_id)
        print("starting_position", starting_position)
        print("alignment_span", alignment_span)
        print("number_of_records", number_of_records)
        print("record_counter", record_counter)
        print("bases", bases)
        print("number_of_blocks", number_of_blocks)
        print("landmark", landmarks)


def read_raw_seq_names_from_sam_header(handle):
    """Offset must be reset after running this."""
    sequence_names = {}
    i = 0
    for line in handle:
        if b'@SQ\t' in line:
            split_SQ = line.split(b'@SQ\t', 1)
            assert len(split_SQ) >= 2
            split_SQ.pop(0)
            for SQ in split_SQ:
                for section in SQ.split(b'\t'):
                    if section.startswith(b'SN:'):
                        sequence_names[section[len(b'SN:'):].decode('utf-8')] = i
                        i += 1
        if sequence_names and b'@SQ\t' not in line:
            break

    return sequence_names


CramLocation = namedtuple("CramLocation", "chr alignment_start alignment_span offset slice_offset slice_size")


def read_gs_cram_file_header(output_cram):
    with open(output_cram, "rb") as fh:
        file_definition(fh)
        container_header(fh, show=False)
        seq_map = read_raw_seq_names_from_sam_header(fh)
    return seq_map


def container_slices(crai_reader, identifiers):
    slices = []
    slice_start = 0
    header_only_section = False
    for i, line in enumerate(crai_reader):
        crai_line = CramLocation(*[int(d) for d in line.split("\t")])
        slice_end = crai_line.offset
        if header_only_section:
            slices.append((slice_start, slice_start + 200))
        else:
            slices.append((slice_start, slice_end))
        slice_start = crai_line.offset
        if crai_line.chr in identifiers:
            header_only_section = False
        else:
            header_only_section = True

    if header_only_section:
        slices.append((slice_start, slice_start + 200))
    else:
        slices.append((slice_start, None))

    return slices


def write_intermediate_cram_output(crai_reader, cram_file, regions):
    for line in crai_reader:
        crai_line = CramLocation(*[int(d) for d in line.split("\t")])
        break
    # TODO: Read as a streamed string
    if cram_file.startswith('gs://'):
        local_cram = download_sliced_gs(gs_path=cram_file, ordered_slices=[(0, crai_line.offset)])
        seq_map = read_gs_cram_file_header(local_cram)
    else:
        seq_map = read_gs_cram_file_header(cram_file)

    identifiers = []
    for region in regions.split(','):
        if ':' in region:
            seq_name, num = region.split(':')
            identifiers.append(seq_map[seq_name])
        else:
            identifiers.append(seq_map[region])
    # identifiers = [seq_map[seq_name] for seq_name, num in (s.split(':') for s in regions.split(','))]
    slices = container_slices(crai_reader, identifiers)
    download_sliced_gs(gs_path=cram_file, ordered_slices=slices, output_filename=tmp_cram)
    return tmp_cram, tmp_crai


def write_final_file_with_samtools(cram: str, crai: str, regions: str, cram_format: bool, output: str):
    region_args = ' '.join(regions.split(',')) if regions else ''
    cram_format = '-C' if cram_format else ''
    cmd = f'samtools view {cram_format} -X {crai} {cram} {region_args} > {output}'
    print(f'Now running: {cmd}')
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if stdout or stderr:
        print(f'\nstdout: {stdout}\n')
        print(f'\nstderr: {stderr}\n')
    else:
        print(f'Output CRAM successfully generated at: {output}')


def make_crai_available(crai: str):
    if crai.startswith('gs://'):
        download_gs(crai, output_filename=tmp_crai)
    elif crai.startswith('file://'):
        os.link(crai[len('file://'):], tmp_crai)
    elif crai.startswith('http://') or crai.startswith('https://'):
        urlretrieve(crai, tmp_crai)
    elif '://' not in crai:
        os.link(crai, tmp_crai)
    else:
        raise NotImplementedError(f'Unsupported format: {crai}')


def write_cram(cram, crai, regions, output, cram_format):
    if regions:
        with open(tmp_crai, "rb") as fh:
            with gzip.GzipFile(fileobj=fh) as gzip_reader:
                with codecs.getreader("ascii")(gzip_reader) as reader:
                    cram, crai = write_intermediate_cram_output(crai_reader=reader, cram_file=cram, regions=regions)

    write_final_file_with_samtools(cram=cram, crai=crai, regions=regions, cram_format=cram_format, output=output)


@xprofile.profile("xsamtools cram view")
def view(cram: str, crai: str, regions: Optional[str], output: str, cram_format: bool):
    if not output:
        time_stamp = str(datetime.datetime.now()).split('.')[0].replace(':', '').replace(' ', '-')
        extension = 'cram' if cram_format else 'sam'
        # NOTE: schema output (gs:// or file://) is preserved
        output = os.path.abspath(f'{time_stamp}.output.{extension}')

    make_crai_available(crai=crai)
    write_cram(cram=cram, crai=crai, regions=regions, output=output, cram_format=cram_format)
    return output
