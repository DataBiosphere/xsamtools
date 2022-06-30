"""
A limited wrapper around "samtools view", but with functions to operate on drs and google cloud bucket keys.
"""
import subprocess


from xsamtools.utils import substitute_drs_and_gs_uris_for_http


def samtools_view(*args: str):
    """
    A limited wrapper around "samtools view", but with functions to operate on drs and google cloud bucket keys.
    """
    args = substitute_drs_and_gs_uris_for_http(args)
    subprocess.run(['samtools', 'view'] + args, check=True)
