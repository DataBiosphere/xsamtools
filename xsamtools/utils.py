import subprocess
import terra_notebook_utils as tnu

from typing import Any


def substitute_drs_and_gs_uris_for_http(*args):
    # TODO: literally input the entire list of exact options for samtools view?
    new_args = []
    for arg in args:
        if arg.strip('"').strip("'").startswith('drs://'):
            new_args.append(tnu.drs.access(arg))
        elif args.strip('"').strip("'").startswith('gs://'):
            new_args.append(tnu.gs.get_signed_url)
        else:
            new_args.append(arg)
    return new_args


def run(cmd: Any, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """
    Subprocess.run() that will default to printing stderr when raising on a non-zero error code.

    Stderr will not be printed and propagated error messages will not be seen if capture_output is not True or
    stderr set to subprocess.PIPE.
    """
    kwargs['check'] = False  # wait and check manually to ensure errors in stderr propagate up
    process = subprocess.run(cmd, **kwargs)
    if process.returncode and check:
        # Error will include the actual error message rather than simply "returned 127".
        if kwargs.get('stderr', False) == subprocess.PIPE or kwargs.get('capture_output', False):
            raise XSamtoolsCalledProcessError(f'Command: "{cmd}" failed with return code: {process.returncode}'
                                              f'\n\n{process.stderr}')
        raise subprocess.CalledProcessError(process.returncode, cmd, process.stdout, process.stderr)
    return process


class XSamtoolsCalledProcessError(Exception):
    pass
