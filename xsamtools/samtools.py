import os
import sys
import typing
import warnings
import subprocess
from typing import Optional

import xsamtools

paths: typing.Dict[str, str] = dict(htsfile=None, bcftools=None)

def _run(cmd: list, **kwargs):
    p = subprocess.run(cmd, **kwargs)
    p.check_returncode()
    return p

def _samtools_binary_path(name):
    path = os.path.join(xsamtools.__path__[0].split("/lib", 1)[0], "bin", name)
    try:
        _run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return path
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    # Look for samtools build directory in repo root (useful for issuing commands from repo)
    paths = dict(bcftools=os.path.join(xsamtools.__path__[0], "..", "build", "bcftools", "bcftools"),
                 htsfile=os.path.join(xsamtools.__path__[0], "..", "build", "htslib", "htsfile"),
                 samtools=os.path.join(xsamtools.__path__[0], "..", "build", "samtools", "samtools"))
    path = paths[name]
    try:
        _run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return path
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    return None

for name in paths:
    paths[name] = _samtools_binary_path(name)
    if paths[name] is None:
        warnings.warn(f"WARNING: {name} unavailable: samtools, htslib, or bcftools build failed during installation. "
                      "          try `pip install -v xsamtools` to diagnose the problem")
