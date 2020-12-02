import subprocess

from typing import Any


def subprocess_w_stderr_run(cmd: Any, stdout: Any = subprocess.PIPE, check: bool = False, shell: bool = True):
    """Subprocess.run() that will print stderr if CalledProcessError is raised."""
    # stderr must be subprocess.PIPE in order to print stderr currently
    process = subprocess.run(cmd, shell=shell, stdout=stdout, stderr=subprocess.PIPE)
    if process.returncode and check:
        raise SubprocessErrorStdError(process.returncode, cmd, process.stdout, process.stderr)
    return process

class SubprocessErrorStdError(subprocess.CalledProcessError):
    """CalledProcessError that also prints stderr."""
    def __str__(self):
        return f"{super().__str__()}\n\n{self.stderr.decode('utf-8', errors='replace')}"
