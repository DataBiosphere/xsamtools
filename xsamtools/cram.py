"""
Tools for working with CRAM files.

CRAM/CRAI spec here:
http://samtools.github.io/hts-specs/CRAMv3.pdf
"""
import os
import subprocess
import datetime
import logging

from tempfile import TemporaryDirectory
from typing import Tuple
from uuid import uuid4
from typing import Optional
from urllib.request import urlretrieve
from terra_notebook_utils import xprofile, gs

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
log = logging.getLogger(__name__)

from xsamtools import gs_utils


def download_full_gs(gs_path: str, output_filename: str = None) -> str:
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else os.path.abspath(os.path.basename(key_name))
    blob = gs_utils._blob_for_url(gs_utils)
    blob.download_to_filename(output_filename)
    log.debug(f'Entire file "{gs_path}" downloaded to: {output_filename}')
    return output_filename


def format_region_args_for_samtools(regions: str) -> str:
    return ' '.join(regions.split(',')) if regions else ''


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
    region_args = format_region_args_for_samtools(regions)
    cram_format_arg = '-C' if cram_format else ''
    if crai:
        crai_arg = f'-X {crai}'
    else:
        log.warning('No crai file present, this may take a while.')
        crai_arg = ''

    cmd = f'samtools view {cram_format_arg} {cram} {crai_arg} {region_args}'

    log.info(f'Now running: {cmd}')
    subprocess.run(cmd, shell=True, stdout=open(output, 'w'), stderr=subprocess.PIPE, check=True)
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


def stage_files_locally(cram: str, crai: Optional[str]) -> Tuple[str, str]:
    # samtools exhibits odd behavior sometimes if the cram and crai are in separate folders
    # keep them together
    staging_dir = f'/tmp/{uuid4()}/'
    os.makedirs(staging_dir)
    staged_cram = os.path.join(staging_dir, f'tmp.cram')
    stage(uri=cram, output=staged_cram)
    if crai:
        staged_crai = os.path.join(staging_dir, f'tmp.crai')
        stage(uri=crai, output=staged_crai)
    else:
        staged_crai = None
    return staged_cram, staged_crai


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
        staged_cram = os.path.join(staging_dir, f'tmp.cram')
        stage(uri=cram, output=staged_cram)
        if crai:
            staged_crai = os.path.join(staging_dir, f'tmp.crai')
            stage(uri=crai, output=staged_crai)
        else:
            staged_crai = None

        write_final_file_with_samtools(cram=staged_cram, crai=staged_crai,
                                       regions=regions, cram_format=cram_format, output=output)

    return output
