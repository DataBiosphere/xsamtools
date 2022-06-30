"""
A limited wrapper around "samtools view", but with functions to operate on drs and google cloud bucket keys.
"""
import sys
import subprocess
import logging

from xsamtools.utils import substitute_drs_and_gs_uris_for_http
from xsamtools import samtools


log = logging.getLogger(__name__)


def samtools_view(preset_args):
    """
    A limited wrapper around "samtools view", but with functions to operate on drs and google cloud bucket keys.
    """
    preset_args = substitute_drs_and_gs_uris_for_http(preset_args)
    cmd = [samtools.paths['samtools'], 'view'] + preset_args
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in process.stdout:
        sys.stdout.write(line.decode('utf-8'))
    if process.returncode:
        sys.stdout.write(f'Command: "{cmd}" failed with return code: {process.returncode}')
        raise subprocess.CalledProcessError(process.returncode, cmd, process.stdout, process.stderr)