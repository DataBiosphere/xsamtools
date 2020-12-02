import subprocess

from typing import Any


def subprocess_w_stderr_run(cmd: Any, stdout: int = subprocess.PIPE, check: bool = False, shell: bool = True):
    """Subprocess.run() that will print stderr if CalledProcessError is raised."""
    # stderr must be subprocess.PIPE in order to print stderr currently
    process = subprocess.run(cmd, shell=shell, stdout=stdout, stderr=subprocess.PIPE)
    if process.returncode and check:
        raise SubprocessErrorStdError(process.returncode, cmd, process.stdout, process.stderr)
    return process

class SubprocessErrorStdError(subprocess.CalledProcessError):
    """
    CalledProcessError that also prints stderr.

    EXAMPLE:
        Traceback (most recent call last):
          File "/home/quokka/git/xsamtools/scrap.py", line 37, in <module>
            raise SubprocessErrorIncludeErrorMessages(p.returncode, cmd, p.stdout, p.stderr)
        __main__.SubprocessErrorIncludeErrorMessages: Command 'samtools view -C /home/ubuntu/xsamtools/test-cram-slicing/NWD938777.b38.irc.v1.cram -X /home/ubuntu/xsamtools/test-cram-slicing/NWD938777.b38.irc.v1.cram.crai chr1 > /home/ubuntu/xsamtools/2020-11-17-062709.output.cram' returned non-zero exit status 2.

        ERROR: b'/bin/sh: 1: cannot create /home/ubuntu/xsamtools/2020-11-17-062709.output.cram: Directory nonexistent\n'
    """
    def __str__(self):
        return f"{super().__str__()}\n\n{self.stderr.decode('utf-8', errors='replace')}"
