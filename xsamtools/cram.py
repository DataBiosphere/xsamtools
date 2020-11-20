"""
Tools for working with CRAM files.

CRAM/CRAI spec here:
http://samtools.github.io/hts-specs/CRAMv3.pdf
"""
import os
import signal
import subprocess
import datetime
import logging

from typing import Optional
from urllib.request import urlretrieve
from google.cloud.storage import Blob
from terra_notebook_utils import xprofile, gs

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
log = logging.getLogger(__name__)


class SubprocessErrorIncludeErrorMessages(subprocess.CalledProcessError):
    """
    CalledProcessError that also prints stderr/stdout.

    EXAMPLE:
        Traceback (most recent call last):
          File "/home/quokka/git/xsamtools/scrap.py", line 37, in <module>
            raise SubprocessErrorIncludeErrorMessages(p.returncode, cmd, p.stdout, p.stderr)
        __main__.SubprocessErrorIncludeErrorMessages: Command 'samtools view -C /home/ubuntu/xsamtools/test-cram-slicing/NWD938777.b38.irc.v1.cram -X /home/ubuntu/xsamtools/test-cram-slicing/NWD938777.b38.irc.v1.cram.crai chr1 > /home/ubuntu/xsamtools/2020-11-17-062709.output.cram' returned non-zero exit status 2.

        ERROR: b'/bin/sh: 1: cannot create /home/ubuntu/xsamtools/2020-11-17-062709.output.cram: Directory nonexistent\n'
    """
    def __str__(self):
        if self.returncode and self.returncode < 0:
            try:
                msg = f"Command '{self.cmd}' died with {signal.Signals(-self.returncode)}."
            except ValueError:
                msg = f"Command '{self.cmd}' died with unknown signal {-self.returncode}."
        else:
            msg = f"Command '{self.cmd}' returned non-zero exit status {self.returncode}."
        return f"{msg}\n\nERROR: {self.stderr}"


def download_full_gs(gs_path: str, output_filename: str = None):
    # TODO: use gs_chunked_io instead
    bucket_name, key_name = gs_path[len('gs://'):].split('/', 1)
    output_filename = output_filename if output_filename else os.path.abspath(os.path.basename(key_name))
    bucket = gs.get_client().get_bucket(bucket_name)
    blob = Blob(key_name, bucket)
    blob.download_to_filename(output_filename)
    log.debug(f'Entire file "{gs_path}" downloaded to: {output_filename}')
    return output_filename


def format_region_args_for_samtools(regions):
    return ' '.join(regions.split(',')) if regions else ''


def format_and_check_cram(cram):
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
                                   output: str):
    region_args = format_region_args_for_samtools(regions)
    cram_format_arg = '-C' if cram_format else ''
    if crai:
        crai_arg = f'-X {crai}'
    else:
        log.warning('No crai file present, this may take a while.')
        crai_arg = ''

    streaming_script = os.path.join(pkg_root, 'scripts/stream_cloud_file')

    if cram.startswith('gs://'):
        # stream the google object into samtools
        cmd = f'{streaming_script} --path {cram} | samtools view {cram_format_arg} - {crai_arg} {region_args}'
    else:
        assert os.path.exists(cram), f'Local file "{cram}" does not exist.'
        cmd = f'samtools view {cram_format_arg} {cram} {crai_arg} {region_args}'

    log.info(f'Now running: {cmd}')
    p = subprocess.run(cmd, shell=True, stdout=open(output, 'w'), stderr=subprocess.PIPE)
    if p.returncode:
        raise SubprocessErrorIncludeErrorMessages(p.returncode, cmd, p.stdout, p.stderr)
    log.debug(f'Output CRAM successfully generated at: {output}')


def stage_crai(crai: str, output: str):
    """
    Always make the crai available locally for samtools to use.

    This also allows the crai to be placed in the same folder as its associated
    cram file, which samtools can be picky about.
    """
    if crai.startswith('gs://'):
        download_full_gs(crai, output_filename=output)
    elif crai.startswith('file://'):
        if os.path.abspath(crai[len('file://'):]) != os.path.abspath(output):
            os.link(crai[len('file://'):], output)
    elif crai.startswith('http://') or crai.startswith('https://'):
        urlretrieve(crai, output)
    elif ':' not in crai:
        if os.path.abspath(crai) != os.path.abspath(output):
            os.link(crai, output)
    else:
        raise NotImplementedError(f'Unsupported format: {crai}')
    return output


def stage_cram(cram: str):
    """Check that the cram is a valid input."""
    if cram.startswith('gs://'):
        return cram
    elif cram.startswith('file://'):
        cram = cram[len('file://'):]

    if ':' in cram:
        raise NotImplementedError(f'Unsupported format: {cram}')

    cram = os.path.abspath(cram)
    assert os.path.exists(cram), f'Input cram does not exist: {cram}'
    return cram


def timestamped_filename(cram_format):
    time_stamp = str(datetime.datetime.now()).split('.')[0].replace(':', '').replace(' ', '-')
    extension = 'cram' if cram_format else 'sam'
    return os.path.abspath(f'{time_stamp}.output.{extension}')


@xprofile.profile("xsamtools cram view")
def view(cram: str,
         crai: Optional[str],
         regions: Optional[str],
         output: Optional[str] = None,
         cram_format: bool = True):
    crai = stage_crai(crai, output=f'{output}.crai') if crai else crai
    cram = stage_cram(cram)

    output = output or timestamped_filename(cram_format)
    output = output[len('file://'):] if output.startswith('file://') else output
    assert ':' not in output, f'Unsupported schema for output: "{output}".\n' \
                              f'Only local file outputs are currently supported.'

    write_final_file_with_samtools(cram=cram, crai=crai, regions=regions, cram_format=cram_format, output=output)
    return output
