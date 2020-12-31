import subprocess

from typing import Any


def run(cmd: Any, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """
    Subprocess.run() that will default to printing stderr when raising on a non-zero error code.

    We cannot use run here as stdout and stderr arguments may not be used with capture_output.

    Stderr will not be printed and error messages will not be seen if capture_output is not True.
    """
    # stderr must be subprocess.PIPE in order to print stderr currently
    kwargs['check'] = False  # wait and check manually to ensure errors in stderr propagate up
    process = subprocess.run(cmd, **kwargs)
    if process.returncode and check:
        # Error will include the actual error message rather than simply "returned 127".
        if kwargs['stderr'] == subprocess.PIPE or kwargs.get('capture_output', False):
            raise XSamtoolsCalledProcessError(f'Command: "{cmd}" failed with return code: {process.returncode}'
                                              f'\n\n{process.stderr}')
        raise subprocess.CalledProcessError(process.returncode, cmd, process.stdout, process.stderr)
    return process

class XSamtoolsCalledProcessError(Exception):
    pass
