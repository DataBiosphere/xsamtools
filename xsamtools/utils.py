import subprocess
import terra_notebook_utils as tnu

from typing import Any


def substitute_drs_and_gs_uris_for_http(args):
    print(args)
    new_args = []
    for arg in args:
        if arg.startswith('-') and '=' in arg:
            key_arg, value_arg = arg.split('=', 1)
        else:
            key_arg, value_arg = None, arg

        if value_arg.strip('"').strip("'").startswith('drs://'):
            value_arg = tnu.drs.access(arg)
        elif value_arg.strip('"').strip("'").startswith('gs://'):
            value_arg = tnu.gs.get_signed_url(arg)

        if key_arg:
            arg = f'{key_arg}={value_arg}'
        else:
            arg = value_arg

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
