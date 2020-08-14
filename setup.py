import os
import glob
import warnings
import subprocess
import traceback
from setuptools import setup, find_packages
from setuptools.command import install, build_py


install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]


def _run(cmd: list, **kwargs):
    p = subprocess.run(cmd, **kwargs)
    p.check_returncode()
    return p

class BuildPy(build_py.build_py):
    def run(self):
        super().run()
        if not self.dry_run:
            try:
                _run(["tar", "xjf", "htslib.tar.bz2", "-C", "build"])
                _run(["tar", "xjf", "bcftools.tar.bz2", "-C", "build"])
                _run(["tar", "xjf", "samtools.tar.bz2", "-C", "build"])
                _run(["./configure"], cwd="build/htslib")
                _run(["./configure"], cwd="build/samtools")
                _run(["make"], cwd="build/htslib")
                _run(["make"], cwd="build/bcftools")
                _run(["make"], cwd="build/samtools")
            except subprocess.CalledProcessError:
                print("Failed to build samtools/htslib/bcftools:")
                traceback.print_exc()
                raise

class Install(install.install):
    def run(self):
        super().run()
        if not self.dry_run:
            root = os.path.dirname(os.path.abspath(__file__))
            bindir = os.path.join(root, os.path.abspath(self.install_scripts))
            datadir = os.path.join(root, os.path.abspath(self.install_data))
            libdir = os.path.join(root, os.path.abspath(self.install_lib))
            includedir = os.path.join(root, os.path.abspath(self.install_headers))
            try:
                _run(["make",
                      f"bindir={bindir}",
                      f"includedir={includedir}",
                      f"libdir={libdir}",
                      f"libexecdir={libdir}",
                      f"datarootdir={datadir}",
                      "INSTALL_MAN=:",
                      "install"], cwd="build/htslib")
                _run(["make",
                      f"bindir={bindir}",
                      f"libdir={libdir}",
                      f"libexecdir={libdir}",
                      "INSTALL_MAN=:",
                      "install"], cwd="build/bcftools")
            except subprocess.CalledProcessError:
                print("Failed to package htslib/bcftools")
                traceback.print_exc()
                raise

with open("README.md") as fh:
    long_description = fh.read()

def get_version():
    filepath = os.path.join(os.path.dirname(__file__), "gs_chunked_io", "version.py")
    if os.path.isfile(filepath):
        # In source distributions or builds, version is available in the generated gs_chunked_io/version.py file
        with open(filepath) as fh:
            version = dict()
            exec(fh.read().strip(), version)
            return version['__version__']
    else:
        p = subprocess.run(["git", "describe", "--tags", "--match", "v*.*.*"], stdout=subprocess.PIPE)
        if 128 == p.returncode:
            warnings.warn('There are no git tags with version information. '
                          'To tag the first commit as v0.0.0 use '
                          '`git tag --annotate "v0.0.0" $(git rev-list --max-parents=0 HEAD) -m "v0.0.0"`')
            return "0"
        else:
            p.check_returncode()
            out = p.stdout.decode("ascii").strip()
            if "-" in out:
                out = out.split("-", 1)[0]
            assert out.startswith("v")
            return out[1:]

setup(
    name='xsamtools',
    version=get_version(),
    description='Lightly modified versions of samtools, htslib, and bcftools.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xbrianh/xsamtools.git',
    author='Brian Hannafious',
    author_email='bhannafi@ucsc.edu',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    scripts=glob.glob('scripts/*'),
    zip_safe=False,
    install_requires=install_requires,
    platforms=['MacOS X', 'Posix'],
    test_suite='test',
    cmdclass=dict(install=Install,
                  build_py=BuildPy),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7'
    ]
)
